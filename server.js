/**
 * Meet - Laravel Signaling Server
 *
 * This Node.js server handles WebSocket signaling for WebRTC.
 * Laravel handles all HTTP routes, pages, API, and database.
 * This server communicates with Laravel via HTTP API calls.
 *
 * Protocol matches the Meet signaling server.
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const axios = require('axios');
const path = require('path');

// Load .env from project root if present (for production)
try {
    const dotenv = require('dotenv');
    dotenv.config({ path: path.join(__dirname, '..', '.env') });
} catch (e) { /* dotenv not installed, use process.env */ }

const app = express();
const server = http.createServer(app);

const PORT = process.env.SIGNALING_PORT || 3001;
const LARAVEL_URL = process.env.LARAVEL_URL || 'http://localhost/meet';

app.use(cors());
app.use(express.json());

// Health check
app.get('/health', (req, res) => {
    res.json({ status: 'ok', rooms: Object.keys(channels).length });
});

// Socket.io setup
const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST'],
    },
    pingTimeout: 60000,
    pingInterval: 25000,
});

// ICE servers config
// Google free STUN for local/LAN + free TURN (Open Relay Project) so that
// connections succeed on weak networks / strict NAT (mobile data, carrier NAT).
// TURN relays media when direct P2P is impossible - this is what fixes
// "video not showing" on poor connections. Override via env if you run your own.
const iceServers = [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
    {
        urls: [
            'turn:standard.relay.metered.ca:80',
            'turn:standard.relay.metered.ca:443',
            'turn:standard.relay.metered.ca:443?transport=tcp',
            'turns:standard.relay.metered.ca:443?transport=tcp',
        ],
        username: process.env.TURN_USERNAME || 'free',
        credential: process.env.TURN_CREDENTIAL || 'free',
    },
];

// Room state (mirrors Meet structure)
// channels[roomId] = { [socketId]: socket }
const channels = {};
// peers[roomId] = { [socketId]: peerInfo }
const peers = {};
// sockets[socketId] = socket
const sockets = {};

/**
 * Send event to a specific peer by socket ID
 */
async function sendToPeer(peerId, socketsMap, event, data) {
    if (socketsMap[peerId]) {
        try {
            socketsMap[peerId].emit(event, data);
        } catch (err) {
            console.error(`[sendToPeer] Error sending to ${peerId}:`, err.message);
        }
    }
}

/**
 * Broadcast to all peers in a room except sender
 */
function broadcastToRoom(roomId, event, data, excludeSocketId = null) {
    if (!channels[roomId]) return;
    Object.keys(channels[roomId]).forEach((peerSocketId) => {
        if (peerSocketId !== excludeSocketId) {
            try {
                channels[roomId][peerSocketId].emit(event, data);
            } catch (err) {
                console.error(`[broadcast] Error to ${peerSocketId}:`, err.message);
            }
        }
    });
}

/**
 * Add peer to channel - notifies all existing peers about the new peer
 * and the new peer about all existing peers
 * (Matches Meet addPeerTo function)
 */
async function addPeerTo(channel, joiningSocket) {
    if (!channels[channel]) return;

    const existingSocketIds = Object.keys(channels[channel]);

    for (const id of existingSocketIds) {
        if (id === joiningSocket.id) continue;

        // Notify existing peer about the new joiner (NO offer)
        await sendToPeer(id, sockets, 'addPeer', {
            peer_id: joiningSocket.id,
            peers: peers[channel] || {},
            should_create_offer: false,
            iceServers: iceServers,
        });

        // Tell the joiner to create an offer to this existing peer
        await sendToPeer(joiningSocket.id, sockets, 'addPeer', {
            peer_id: id,
            peers: peers[channel] || {},
            should_create_offer: true,
            iceServers: iceServers,
        });

        console.log(`[addPeer] ${joiningSocket.id} <--> ${id} in ${channel}`);
    }
}

/**
 * Remove peer from channel
 */
async function removePeerFrom(channel, socket, reason = 'unknown') {
    if (!channels[channel] || !channels[channel][socket.id]) return;

    try {
        // Notify all remaining peers to remove this peer
        for (const id in channels[channel]) {
            if (id !== socket.id) {
                await sendToPeer(id, sockets, 'removePeer', { peer_id: socket.id });
            }
        }

        // Clean up
        delete channels[channel][socket.id];
        if (peers[channel]) delete peers[channel][socket.id];

        socket.leave(channel);

        // If room is empty, clean up
        if (Object.keys(channels[channel]).length === 0) {
            delete channels[channel];
            delete peers[channel];
            console.log(`[Room Empty] Room ${channel} deleted`);
        }

        console.log(`[RemovePeer] ${socket.id} removed from ${channel} (${reason})`);
    } catch (err) {
        console.error(`[removePeerFrom] Error:`, err.message);
    }
}

/**
 * Helper: Find which room and peer info for a socket
 */
function findPeerBySocket(socketId) {
    for (const roomId in peers) {
        if (peers[roomId][socketId]) {
            return { roomId, info: peers[roomId][socketId] };
        }
    }
    return null;
}

/**
 * Helper: Find room for a socket
 */
function findRoomBySocket(socketId) {
    for (const roomId in channels) {
        if (channels[roomId][socketId]) {
            return roomId;
        }
    }
    return null;
}

// Socket.io connection handler
io.on('connection', async (socket) => {
    console.log(`[Connect] ${socket.id}`);
    sockets[socket.id] = socket;

    // Track which channels this socket is in
    socket.channels = {};

    /**
     * DATA EVENT - Handle incoming data with callback
     * This is the critical event that checkUserName() uses
     */
    socket.on('data', async (dataObj, cb) => {
        try {
            const data = dataObj;
            if (!data || !data.method) return;

            const { room_id, peer_id, peer_name, method } = data;

            console.log(`[Data] ${method}`, { room_id, peer_name });

            switch (method) {
                case 'checkPeerName': {
                    // Check if peer name is already in use in the room
                    if (peers[room_id]) {
                        for (let id in peers[room_id]) {
                            if (peer_id !== id && peers[room_id][id]['peer_name'] === peer_name) {
                                console.log(`[checkPeerName] Name "${peer_name}" already in use in room ${room_id}`);
                                if (cb) cb(true);
                                return;
                            }
                        }
                    }
                    if (cb) cb(false);
                    break;
                }
                default:
                    console.log(`[Data] Unknown method: ${method}`);
                    if (cb) cb(false);
                    break;
            }
        } catch (err) {
            console.error('[Data Error]', err.message);
            if (cb) cb(false);
        }
    });

    /**
     * JOIN ROOM
     * Client sends: { channel, channel_password, peer_info, peer_uuid, peer_name, ... }
     */
    socket.on('join', async (cfg) => {
        try {
            const {
                channel,
                channel_password,
                peer_uuid,
                peer_name,
                peer_avatar,
                peer_token,
                peer_video,
                peer_audio,
                peer_video_status,
                peer_audio_status,
                peer_screen_status,
                peer_hand_status,
                peer_rec_status,
                peer_privacy_status,
                peer_info,
            } = cfg;

            if (!channel) {
                console.log('[Join] No channel name provided');
                return;
            }

            const roomId = channel;
            console.log(`[Join] ${peer_name} joining room ${roomId}`);

            // Initialize room structures
            if (!channels[roomId]) channels[roomId] = {};
            if (!peers[roomId]) peers[roomId] = {};

            // Store peer info (matches Meet format)
            peers[roomId][socket.id] = {
                peer_name: peer_name || 'Anonymous',
                peer_avatar: peer_avatar || '',
                peer_video: peer_video || false,
                peer_audio: peer_audio || false,
                peer_video_status: peer_video_status || false,
                peer_audio_status: peer_audio_status || false,
                peer_screen_status: peer_screen_status || false,
                peer_hand_status: peer_hand_status || false,
                peer_rec_status: peer_rec_status || false,
                peer_privacy_status: peer_privacy_status || false,
                os: peer_info ? `${peer_info.osName || ''} ${peer_info.osVersion || ''}`.trim() : '',
                browser: peer_info ? `${peer_info.browserName || ''} ${peer_info.browserVersion || ''}`.trim() : '',
                extras: peer_info ? peer_info.extras || {} : {},
            };

            // Add to channels
            channels[roomId][socket.id] = socket;
            socket.channels[roomId] = roomId;

            // Join the Socket.io room
            socket.join(roomId);

            // Send addPeer events (existing peers notified, new peer gets offers)
            await addPeerTo(roomId, socket);

            // Send serverInfo to the joining peer
            const peerCounts = Object.keys(peers[roomId] || {}).length;
            await sendToPeer(socket.id, sockets, 'serverInfo', {
                peers_count: peerCounts,
                host_protected: false,
                user_auth: false,
                is_presenter: true,
                join_locked: false,
                survey: { active: false, url: '' },
                redirect: { active: false, url: '' },
                maxRoomParticipants: 0,
                whisper: { enabled: false, segmentSeconds: 0 },
            });

            console.log(`[Join] ${peer_name} joined room ${roomId} (${peerCounts} peers)`);

            // Notify Laravel (optional)
            try {
                await axios.post(`${LARAVEL_URL}/api/v1/internal/peer-joined`, {
                    room_id: roomId,
                    peer_id: socket.id,
                    socket_id: socket.id,
                    peer_name,
                }).catch(() => {});
            } catch (e) { /* Laravel integration optional */ }
        } catch (err) {
            console.error('[Join Error]', err.message);
            socket.emit('serverInfo', { error: err.message });
        }
    });

    /**
     * RELAY ICE CANDIDATE
     * Client sends: { peer_id, ice_candidate }
     * Server emits: 'iceCandidate' to target peer
     */
    socket.on('relayICE', async (config) => {
        const { peer_id, ice_candidate } = config;

        await sendToPeer(peer_id, sockets, 'iceCandidate', {
            peer_id: socket.id,
            ice_candidate: ice_candidate,
        });
    });

    /**
     * RELAY SDP (Session Description Protocol)
     * Client sends: { peer_id, session_description }
     * Server emits: 'sessionDescription' to target peer
     */
    socket.on('relaySDP', async (config) => {
        const { peer_id, session_description } = config;

        console.log(`[relaySDP] ${socket.id} -> ${peer_id} (${session_description?.type})`);

        await sendToPeer(peer_id, sockets, 'sessionDescription', {
            peer_id: socket.id,
            session_description: session_description,
        });
    });

    /**
     * ROOM ACTION (lock, unlock, password change, etc.)
     */
    socket.on('roomAction', async (cfg) => {
        const peer = findPeerBySocket(socket.id);
        if (!peer) return;

        broadcastToRoom(peer.roomId, 'roomAction', {
            ...cfg,
            peer_id: socket.id,
            peer_name: peer.info.peer_name,
        });
    });

    /**
     * PEER NAME UPDATE
     */
    socket.on('peerName', async (cfg) => {
        const { room_id, peer_name_new, peer_name_old, peer_avatar } = cfg;
        const roomId = room_id || findRoomBySocket(socket.id);

        if (roomId && peers[roomId] && peers[roomId][socket.id]) {
            peers[roomId][socket.id].peer_name = peer_name_new;
            if (peer_avatar) peers[roomId][socket.id].peer_avatar = peer_avatar;
        }

        broadcastToRoom(roomId, 'peerName', {
            peer_id: socket.id,
            peer_name: peer_name_new,
        }, socket.id);
    });

    /**
     * CHAT MESSAGE
     */
    socket.on('message', async (message) => {
        const peer = findPeerBySocket(socket.id);
        if (!peer) return;

        broadcastToRoom(peer.roomId, 'message', {
            peer_id: socket.id,
            peer_name: peer.info.peer_name,
            msg: message.msg || '',
            time: new Date().toISOString(),
            room_id: peer.roomId,
        });
    });

    /**
     * CMD (commands: toggle audio/video, etc.)
     */
    socket.on('cmd', async (cfg) => {
        const peer = findPeerBySocket(socket.id);
        if (!peer) return;

        broadcastToRoom(peer.roomId, 'cmd', {
            ...cfg,
            peer_id: socket.id,
            peer_name: peer.info.peer_name,
        }, socket.id);
    });

    /**
     * PEER STATUS (audio/video/screen toggle)
     */
    socket.on('peerStatus', async (cfg) => {
        const peer = findPeerBySocket(socket.id);
        if (!peer) return;

        // Update peer status in room
        if (peers[peer.roomId] && peers[peer.roomId][socket.id]) {
            if (cfg.element) {
                peers[peer.roomId][socket.id][cfg.element] = cfg.status;
            }
        }

        broadcastToRoom(peer.roomId, 'peerStatus', {
            peer_id: socket.id,
            peer_name: peer.info.peer_name,
            ...cfg,
        }, socket.id);
    });

    /**
     * PEER ACTION (mute, hand raise, etc.)
     */
    socket.on('peerAction', async (cfg) => {
        const peer = findPeerBySocket(socket.id);
        if (!peer) return;

        broadcastToRoom(peer.roomId, 'peerAction', {
            ...cfg,
            peer_id: socket.id,
            peer_name: peer.info.peer_name,
        });
    });

    /**
     * KICK OUT
     */
    socket.on('kickOut', async (cfg) => {
        const peer = findPeerBySocket(socket.id);
        if (!peer) return;

        const targetSocketId = cfg.peer_id;
        const targetSocket = sockets[targetSocketId];
        if (targetSocket) {
            targetSocket.emit('kickOut', {
                peer_id: socket.id,
                peer_name: peer.info.peer_name,
                room_id: peer.roomId,
            });
        }

        broadcastToRoom(peer.roomId, 'kickOut', {
            ...cfg,
            peer_id: socket.id,
            peer_name: peer.info.peer_name,
        }, socket.id);
    });

    /**
     * FILE INFO
     */
    socket.on('fileInfo', async (cfg) => {
        const peer = findPeerBySocket(socket.id);
        if (!peer) return;

        broadcastToRoom(peer.roomId, 'fileInfo', {
            ...cfg,
            peer_id: socket.id,
            peer_name: peer.info.peer_name,
        });
    });

    /**
     * FILE ABORT
     */
    socket.on('fileAbort', async (cfg) => {
        const peer = findPeerBySocket(socket.id);
        if (!peer) return;

        broadcastToRoom(peer.roomId, 'fileAbort', {
            ...cfg,
            peer_id: socket.id,
        });
    });

    /**
     * FILE RECEIVE ABORT
     */
    socket.on('fileReceiveAbort', async (cfg) => {
        const peer = findPeerBySocket(socket.id);
        if (!peer) return;

        broadcastToRoom(peer.roomId, 'fileReceiveAbort', {
            ...cfg,
            peer_id: socket.id,
        });
    });

    /**
     * VIDEO PLAYER (YouTube sharing etc.)
     */
    socket.on('videoPlayer', async (cfg) => {
        const peer = findPeerBySocket(socket.id);
        if (!peer) return;

        broadcastToRoom(peer.roomId, 'videoPlayer', {
            ...cfg,
            peer_id: socket.id,
            peer_name: peer.info.peer_name,
        });
    });

    /**
     * WHITEBOARD
     */
    socket.on('wbCanvasToJson', async (cfg) => {
        const peer = findPeerBySocket(socket.id);
        if (!peer) return;

        broadcastToRoom(peer.roomId, 'wbCanvasToJson', {
            ...cfg,
            peer_id: socket.id,
        }, socket.id);
    });

    socket.on('whiteboardAction', async (cfg) => {
        const peer = findPeerBySocket(socket.id);
        if (!peer) return;

        broadcastToRoom(peer.roomId, 'whiteboardAction', {
            ...cfg,
            peer_id: socket.id,
            peer_name: peer.info.peer_name,
        });
    });

    /**
     * CAPTION (live captions)
     */
    socket.on('caption', async (cfg) => {
        const peer = findPeerBySocket(socket.id);
        if (!peer) return;

        broadcastToRoom(peer.roomId, 'caption', {
            ...cfg,
            peer_id: socket.id,
        }, socket.id);
    });

    /**
     * DISCONNECT
     * Emits 'removePeer' (not 'peerLeft') to match client expectations
     */
    socket.on('disconnect', async (reason) => {
        console.log(`[Disconnect] ${socket.id} (${reason})`);

        // Remove from all channels
        for (const channelName in socket.channels) {
            await removePeerFrom(channelName, socket, reason);
        }

        // Notify Laravel
        try {
            const peerData = findPeerBySocket(socket.id);
            if (peerData) {
                await axios.post(`${LARAVEL_URL}/api/v1/internal/peer-left`, {
                    room_id: peerData.roomId,
                    peer_id: socket.id,
                    socket_id: socket.id,
                }).catch(() => {});
            }
        } catch (e) { /* optional */ }

        // Clean up
        delete sockets[socket.id];
    });
});

// Start server
server.listen(PORT, () => {
    console.log(`\n🎥 Meet Signaling Server`);
    console.log(`   WebSocket: ws://localhost:${PORT}`);
    console.log(`   Health:    http://localhost:${PORT}/health`);
    console.log(`   Laravel:   ${LARAVEL_URL}`);
    console.log(`   ICE:       ${iceServers.map(s => s.urls).join(', ')}\n`);
});
