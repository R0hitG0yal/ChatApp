const cluster = require('node:cluster');
const http = require('node:http');
const numCPUs = require('node:os').availableParallelism();
const process = require('node:process');
const { setupMaster, setupWorker } = require("@socket.io/sticky");
const { createAdapter, setupPrimary } = require("@socket.io/cluster-adapter");
const { Server } = require("socket.io");
const express = require("express");
const Redis = require("ioredis");

// Constants
const PORT = process.env.PORT || 3000;
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const CHAT_HISTORY_KEY = 'chat_messages';
const MAX_CHAT_HISTORY = 100; // Limit the number of stored messages

/**
 * Configure Redis client with error handling
 * @returns {Redis} Configured Redis client
 */
function setupRedis() {
    const redisClient = new Redis(REDIS_URL, {
        retryStrategy: (times) => {
            const delay = Math.min(times * 50, 2000);
            return delay;
        },
        maxRetriesPerRequest: 3
    });

    redisClient.on('error', (err) => {
        console.error('Redis Client Error:', err);
        if (err.code == "EPIPE") {
            process.exit(0);
        }

    });

    redisClient.on('connect', () => {
        console.log('Redis Client Connected');
    });

    return redisClient;
}

/**
 * Setup primary/master process
 */
async function setupPrimaryProcess() {
    console.log(`Primary ${process.pid} is running`);

    const httpServer = http.createServer();

    // Setup sticky sessions
    setupMaster(httpServer, {
        loadBalancingMethod: "least-connection"
    });

    // Setup cluster communication
    setupPrimary();
    cluster.setupPrimary({
        serialization: "advanced"
    });

    // Fork workers
    for (let i = 0; i < numCPUs; i++) {
        cluster.fork();
    }

    // Handle worker events
    cluster.on('exit', (worker, code, signal) => {
        console.log(`Worker ${worker.process.pid} died with code ${code} and signal ${signal}`);
        console.log('Forking new worker...');
        cluster.fork();
    });

    httpServer.listen(PORT, () => {
        console.log(`Primary server listening on port ${PORT}`);
    });
}

/**
 * Setup worker process
 */
async function setupWorkerProcess() {
    console.log(`Worker ${process.pid} started`);

    const redisClient = setupRedis();
    const app = express();
    const httpServer = http.createServer(app);
    const io = new Server(httpServer, {
        cors: {
            origin: process.env.CORS_ORIGIN || "*",
            methods: ["GET", "POST"]
        }
    });

    // Setup Socket.IO adapter
    io.adapter(createAdapter());
    setupWorker(io);

    // Serve static files
    app.use(express.static('public'));

    // Handle Socket.IO connections
    io.on("connection", async (socket) => {
        try {
            // Send chat history
            const existingMessages = await redisClient.lrange(CHAT_HISTORY_KEY, 0, MAX_CHAT_HISTORY - 1);
            // console.log(existingMessages);
            const parsedMessages = existingMessages
                .map(item => {
                    try {
                        return JSON.parse(item);
                    } catch (err) {
                        console.error('Error parsing message:', err);
                        return null;
                    }
                })
                .filter(Boolean)
                .reverse();
            console.log(parsedMessages);

            socket.emit("historical_messages", parsedMessages);

            // Handle new messages
            socket.on("message", async (data) => {
                try {
                    if (!data || typeof data !== 'object') {
                        throw new Error('Invalid message format');
                    }

                    const messageData = {
                        ...data,
                        timestamp: Date.now(),
                        workerId: process.pid
                    };

                    console.log(`Message received at worker ${process.pid}:`, messageData);

                    // Store in Redis with TTL
                    await redisClient.lpush(CHAT_HISTORY_KEY, JSON.stringify(messageData));
                    await redisClient.ltrim(CHAT_HISTORY_KEY, 0, MAX_CHAT_HISTORY - 1);

                    // Broadcast to all clients
                    io.emit("message", messageData);
                } catch (err) {
                    console.error('Error handling message:', err);
                    socket.emit("error", "Failed to process message");
                }
            });

            // Handle disconnection
            socket.on("disconnect", () => {
                console.log(`Client disconnected from worker ${process.pid}`);
            });

        } catch (err) {
            console.error('Error in socket connection:', err);
            socket.emit("error", "Internal server error");
        }
    });

    // Health check endpoint
    app.get('/health', (req, res) => {
        res.json({
            status: 'ok',
            workerId: process.pid,
            uptime: process.uptime()
        });
    });
}

// Main execution
if (cluster.isPrimary) {
    setupPrimaryProcess().catch(err => {
        console.error('Failed to setup primary process:', err);
        process.exit(1);
    });
} else {
    setupWorkerProcess().catch(err => {
        console.error('Failed to setup worker process:', err);
        process.exit(1);
    });
}