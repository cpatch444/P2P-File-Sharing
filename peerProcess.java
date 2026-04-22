import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class peerProcess {
    private final String peerId;
    private final CommonConfig commonConfig;
    private final PeerInfoConfig peerInfoConfig;
    private final PeerInfo myInfo;

    private final String peerDir;
    private final int numPieces;
    private final int pieceSize;
    private final int fileSize;

    private final BitSet bitfield;
    private final Object bitfieldLock = new Object();
    private final AtomicBoolean hasCompleteFile = new AtomicBoolean(false);
    private final Set<Integer> requestedPieces = ConcurrentHashMap.newKeySet();
    private final Random random = new Random();

    private final Map<String, Connection> connections = new ConcurrentHashMap<>();
    private final ChokeManager chokeManager;
    private final PrintWriter logWriter;

    private final Map<String, Boolean> peerComplete = new ConcurrentHashMap<>();
    private volatile boolean terminated = false;
    private final int myIndex;

    public peerProcess(String peerId) throws Exception {
        this.peerId = peerId;
        this.commonConfig = new CommonConfig("Common.cfg");
        this.peerInfoConfig = new PeerInfoConfig("PeerInfo.cfg");
        this.myInfo = peerInfoConfig.getPeerById(peerId);
        if (myInfo == null) {
            throw new IllegalArgumentException("Peer " + peerId + " not in PeerInfo.cfg");
        }
        int idx = -1;
        List<PeerInfo> peers = peerInfoConfig.getPeers();
        for (int i = 0; i < peers.size(); i++) {
            if (peers.get(i).getPeerId().equals(peerId)) {
                idx = i;
                break;
            }
        }
        if (idx < 0) {
            throw new IllegalArgumentException("Peer " + peerId + " not found in PeerInfo.cfg");
        }
        this.myIndex = idx;

        this.peerDir = "peer_" + peerId;
        Files.createDirectories(Paths.get(peerDir));

        this.fileSize = commonConfig.getFileSize();
        this.pieceSize = commonConfig.getPieceSize();
        this.numPieces = commonConfig.getNumPieces();
        this.bitfield = new BitSet(numPieces);

        this.logWriter = new PrintWriter(new FileWriter("log_peer_" + peerId + ".log"), true);

        this.chokeManager = new ChokeManager(
                commonConfig.getNumberOfPreferredNeighbors(),
                new ChokeListenerImpl()
        );

        // If this peer starts with the complete file, mark all pieces as present
        Path complete = Paths.get(peerDir, commonConfig.getFileName());
        if (myInfo.File() && Files.exists(complete)) {
            int pieces = (fileSize + pieceSize - 1) / pieceSize;
            for (int i = 0; i < pieces; i++) {
                bitfield.set(i);
            }
            hasCompleteFile.set(true);
        }

        for (PeerInfo p : peerInfoConfig.getPeers()) {
            peerComplete.put(p.getPeerId(), false);
        }
        peerComplete.put(peerId, hasCompleteFile.get());
    }

    public void run() throws Exception {
        ServerSocket serverSocket = new ServerSocket(myInfo.getPort());
        new Thread(() -> acceptConnections(serverSocket)).start();

        List<PeerInfo> peers = peerInfoConfig.getPeers();
        for (int i = 0; i < myIndex; i++) {
            final PeerInfo target = peers.get(i);
            new Thread(() -> connectWithRetry(target)).start();
        }

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        scheduler.scheduleAtFixedRate(
                () -> chokeManager.selectPreferredNeighbors(),
                commonConfig.getUnchokingInterval(),
                commonConfig.getUnchokingInterval(),
                TimeUnit.SECONDS
        );
        scheduler.scheduleAtFixedRate(
                () -> chokeManager.selectOptimisticNeighbor(),
                commonConfig.getOptimisticUnchokingInterval(),
                commonConfig.getOptimisticUnchokingInterval(),
                TimeUnit.SECONDS
        );

        Thread.currentThread().join();
    }

    private void acceptConnections(ServerSocket serverSocket) {
        try {
            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(() -> handleIncoming(socket)).start();
            }
        } catch (IOException e) {
            log("Server error: " + e.getMessage());
        }
    }

    private void handleIncoming(Socket socket) {
        try {
            InputStream in = socket.getInputStream();
            Handshake hs = Handshake.decode(in);
            String remoteId = String.valueOf(hs.getPeerId());
            if (!isValidIncomingPeer(remoteId)) {
                throw new IOException("Unexpected incoming handshake peer: " + remoteId);
            }

            OutputStream out = socket.getOutputStream();
            out.write(new Handshake(Integer.parseInt(peerId)).encode());
            out.flush();

            log("Peer " + peerId + " is connected from Peer " + remoteId + ".");
            startConnection(socket, in, out, remoteId);
        } catch (Exception e) {
            log("Incoming connection error: " + e.getMessage());
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private void connectToPeer(PeerInfo p) {
        if (connections.containsKey(p.getPeerId()) || terminated) return;
        try {
            Socket socket = new Socket(p.getHostName(), p.getPort());
            log("Peer " + peerId + " makes a connection to Peer " + p.getPeerId() + ".");
            OutputStream out = socket.getOutputStream();
            out.write(new Handshake(Integer.parseInt(peerId)).encode());
            out.flush();

            InputStream in = socket.getInputStream();
            Handshake hs = Handshake.decode(in);
            String remoteId = String.valueOf(hs.getPeerId());
            if (!remoteId.equals(p.getPeerId())) {
                socket.close();
                return;
            }

            startConnection(socket, in, out, remoteId);
        } catch (Exception e) {
            log("Failed to connect to peer " + p.getPeerId() + ": " + e.getMessage());
        }
    }

    private void connectWithRetry(PeerInfo p) {
        while (!terminated && !connections.containsKey(p.getPeerId())) {
            connectToPeer(p);
            if (connections.containsKey(p.getPeerId()) || terminated) return;
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void startConnection(Socket socket, InputStream in, OutputStream out, String remoteId) {
        Connection conn = new Connection(socket, in, out, remoteId);
        if (connections.putIfAbsent(remoteId, conn) != null) {
            try { socket.close(); } catch (IOException ignored) {}
            return;
        }
        chokeManager.addPeer(remoteId);

        // send bitfield
        sendOrHandleFailure(conn, Message.bitfield(encodeBitfield()), "bitfield");

        new Thread(() -> receiveLoop(conn)).start();
    }

    private void receiveLoop(Connection conn) {
        String remoteId = conn.remotePeerId;
        try {
            while (true) {
                Message msg = Message.receive(conn.in);
                switch (msg.getType()) {
                    case MessageTypes.CHOKE:
                        conn.unchokedByThem = false;
                        releaseOutstandingRequest(conn);
                        log("Peer " + peerId + " is choked by " + remoteId + ".");
                        break;
                    case MessageTypes.UNCHOKE:
                        conn.unchokedByThem = true;
                        log("Peer " + peerId + " is unchoked by " + remoteId + ".");
                        requestNextPiece(conn);
                        break;
                    case MessageTypes.INTERESTED:
                        if (!conn.remoteInterestedInMe) {
                            log("Peer " + peerId + " received the 'interested' message from " + remoteId + ".");
                        }
                        conn.remoteInterestedInMe = true;
                        chokeManager.setInterested(remoteId, true);
                        break;
                    case MessageTypes.NOT_INTERESTED:
                        if (conn.remoteInterestedInMe) {
                            log("Peer " + peerId + " received the 'not interested' message from " + remoteId + ".");
                        }
                        conn.remoteInterestedInMe = false;
                        chokeManager.setInterested(remoteId, false);
                        break;
                    case MessageTypes.HAVE:
                        int haveIdx = msg.getPieceIndex();
                        if (!conn.remoteBitfield.get(haveIdx)) {
                            log("Peer " + peerId + " received the 'have' message from " + remoteId + " for the piece " + haveIdx + ".");
                        }
                        conn.remoteBitfield.set(haveIdx);
                        updateInterest(conn);
                        if (conn.remoteBitfield.cardinality() == numPieces) {
                            peerComplete.put(remoteId, true);
                            checkTermination();
                        }
                        break;
                    case MessageTypes.BITFIELD:
                        decodeBitfieldInto(msg.getPayload(), conn.remoteBitfield);
                        log("Peer " + peerId + " received the 'bitfield' message from " + remoteId);
                        updateInterest(conn);
                        if (conn.remoteBitfield.cardinality() == numPieces) {
                            peerComplete.put(remoteId, true);
                            checkTermination();
                        }
                        if (conn.unchokedByThem) requestNextPiece(conn);
                        break;
                    case MessageTypes.REQUEST:
                        int reqIdx = msg.getPieceIndex();
                        if (chokeManager.isUnchoked(remoteId) && hasPiece(reqIdx)) {
                            byte[] piece = readPiece(reqIdx);
                            if (piece != null) sendOrHandleFailure(conn, Message.piece(reqIdx, piece), "piece");
                        }
                        break;
                    case MessageTypes.PIECE:
                        int pIdx = msg.getPieceIndex();
                        byte[] content = msg.getPieceContent();
                        Integer outstanding = conn.outstandingRequest;
                        if (outstanding == null || outstanding != pIdx) {
                            log("Ignoring unexpected piece " + pIdx + " from Peer " + remoteId + ".");
                            break;
                        }
                        releaseOutstandingRequest(conn);
                        writePiece(pIdx, content);
                        setPiece(pIdx);
                        chokeManager.recordDownload(remoteId, 4 + content.length);
                        log("Peer " + peerId + " has downloaded the piece " + pIdx + " from " + remoteId + ". Now the number of pieces it has is " + localPieceCount() + ".");
                        broadcastHave(pIdx);
                        updateInterestForAllConnections();
                        if (localPieceCount() == numPieces && !hasCompleteFile.get()) {
                            hasCompleteFile.set(true);
                            finalizeDownload();
                            log("Peer " + peerId + " has downloaded the complete file.");
                            peerComplete.put(peerId, true);
                            checkTermination();
                        }
                        requestNextPiece(conn);
                        break;
                    default:
                        break;
                }
            }
        } catch (IOException e) {
            releaseOutstandingRequest(conn);
            connections.remove(remoteId);
            chokeManager.removePeer(remoteId);
        }
    }

    private void updateInterest(Connection conn) {
        boolean interested = false;
        for (int i = 0; i < numPieces; i++) {
            if (!hasPiece(i) && conn.remoteBitfield.get(i)) {
                interested = true;
                break;
            }
        }
        sendOrHandleFailure(conn, interested ? Message.interested() : Message.notInterested(), "interest");
    }

    private void requestNextPiece(Connection conn) {
        if (!conn.unchokedByThem) return;
        if (conn.outstandingRequest != null) return;
        List<Integer> candidates = new ArrayList<>();
        for (int i = 0; i < numPieces; i++) {
            if (!hasPiece(i) && conn.remoteBitfield.get(i) && !requestedPieces.contains(i)) {
                candidates.add(i);
            }
        }
        if (candidates.isEmpty()) return;
        int pieceIdx = candidates.get(random.nextInt(candidates.size()));
        if (requestedPieces.add(pieceIdx)) {
            conn.outstandingRequest = pieceIdx;
            sendOrHandleFailure(conn, Message.request(pieceIdx), "request");
        }
    }

    private void releaseOutstandingRequest(Connection conn) {
        Integer outstanding = conn.outstandingRequest;
        if (outstanding != null) {
            requestedPieces.remove(outstanding);
            conn.outstandingRequest = null;
        }
    }

    private void updateInterestForAllConnections() {
        for (Connection c : connections.values()) {
            updateInterest(c);
        }
    }

    private boolean isValidIncomingPeer(String remoteId) {
        if (remoteId.equals(peerId)) return false;
        if (connections.containsKey(remoteId)) return false;
        List<PeerInfo> peers = peerInfoConfig.getPeers();
        int remoteIndex = -1;
        for (int i = 0; i < peers.size(); i++) {
            if (peers.get(i).getPeerId().equals(remoteId)) {
                remoteIndex = i;
                break;
            }
        }
        return remoteIndex > myIndex;
    }

    private void broadcastHave(int pieceIdx) {
        Message have = Message.have(pieceIdx);
        for (Connection c : connections.values()) {
            sendOrHandleFailure(c, have, "have");
        }
    }

    private boolean sendOrHandleFailure(Connection conn, Message msg, String context) {
        if (conn.send(msg)) return true;
        handleSendFailure(conn, context);
        return false;
    }

    private void handleSendFailure(Connection conn, String context) {
        releaseOutstandingRequest(conn);
        connections.remove(conn.remotePeerId, conn);
        chokeManager.removePeer(conn.remotePeerId);
        log("Send failed for " + context + " message to Peer " + conn.remotePeerId + ".");
        try { conn.socket.close(); } catch (IOException ignored) {}
    }

    private byte[] encodeBitfield() {
        int len = (numPieces + 7) / 8;
        byte[] bf = new byte[len];
        synchronized (bitfieldLock) {
            for (int i = 0; i < numPieces; i++) {
                if (bitfield.get(i)) {
                    bf[i / 8] |= (1 << (7 - (i % 8)));
                }
            }
        }
        return bf;
    }

    private void decodeBitfieldInto(byte[] bf, BitSet target) {
        for (int i = 0; i < numPieces && i < bf.length * 8; i++) {
            if ((bf[i / 8] & (1 << (7 - (i % 8)))) != 0) {
                target.set(i);
            }
        }
    }

    private byte[] readPiece(int index) throws IOException {
        Path path = hasCompleteFile.get()
                ? Paths.get(peerDir, commonConfig.getFileName())
                : Paths.get(peerDir, commonConfig.getFileName() + ".downloading");
        if (!Files.exists(path)) return null;
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            long pos = (long) index * pieceSize;
            if (pos >= raf.length()) return null;
            int len = (int) Math.min(pieceSize, raf.length() - pos);
            byte[] buf = new byte[len];
            raf.seek(pos);
            raf.readFully(buf);
            return buf;
        }
    }

    private void writePiece(int index, byte[] content) throws IOException {
        Path path = Paths.get(peerDir, commonConfig.getFileName() + ".downloading");
        if (!Files.exists(path)) {
            try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
                raf.setLength(fileSize);
            }
        }
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            raf.seek((long) index * pieceSize);
            raf.write(content);
        }
    }

    private boolean hasPiece(int idx) {
        synchronized (bitfieldLock) {
            return bitfield.get(idx);
        }
    }

    private void setPiece(int idx) {
        synchronized (bitfieldLock) {
            bitfield.set(idx);
        }
    }

    private int localPieceCount() {
        synchronized (bitfieldLock) {
            return bitfield.cardinality();
        }
    }

    private void finalizeDownload() throws IOException {
        Path src = Paths.get(peerDir, commonConfig.getFileName() + ".downloading");
        Path dst = Paths.get(peerDir, commonConfig.getFileName());
        if (Files.exists(src)) {
            Files.move(src, dst, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static final DateTimeFormatter LOG_TIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private void log(String msg) {
        logWriter.println("[" + LocalDateTime.now().format(LOG_TIME_FMT) + "]: " + msg);
    }

    private void checkTermination() {
        if (terminated) return;
        for (Boolean b : peerComplete.values()) {
            if (!Boolean.TRUE.equals(b)) return;
        }
        terminated = true;
        shutdown();
    }

    private void shutdown() {
        logWriter.flush();
        logWriter.close();
        for (Connection c : connections.values()) {
            try { c.socket.close(); } catch (IOException ignored) {}
        }
        System.exit(0);
    }

    private class ChokeListenerImpl implements ChokeManager.ChokeListener {
        @Override
        public void onUnchoke(String pid) {
            Connection c = connections.get(pid);
            if (c != null) sendOrHandleFailure(c, Message.unchoke(), "unchoke");
        }
        @Override
        public void onChoke(String pid) {
            Connection c = connections.get(pid);
            if (c != null) sendOrHandleFailure(c, Message.choke(), "choke");
        }
        @Override
        public void onPreferredNeighborsChanged(List<String> preferred) {
            log("Peer " + peerId + " has the preferred neighbors " + String.join(",", preferred) + ".");
        }
        @Override
        public void onOptimisticNeighborChanged(String pid) {
            log("Peer " + peerId + " has the optimistically unchoked neighbor " + pid + ".");
        }
        @Override
        public boolean hasCompleteFile() {
            return hasCompleteFile.get();
        }
    }

    private static class Connection {
        final Socket socket;
        final InputStream in;
        final OutputStream out;
        final String remotePeerId;
        final BitSet remoteBitfield = new BitSet();
        volatile boolean remoteInterestedInMe = false;
        volatile boolean unchokedByThem = false;
        volatile Integer outstandingRequest = null;
        private final Object sendLock = new Object();

        Connection(Socket socket, InputStream in, OutputStream out, String remotePeerId) {
            this.socket = socket;
            this.in = in;
            this.out = out;
            this.remotePeerId = remotePeerId;
        }

        boolean send(Message m) {
            synchronized (sendLock) {
                try {
                    m.send(out);
                    return true;
                } catch (IOException ignored) {}
                return false;
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java peerProcess <peerId>");
            System.exit(1);
        }
        try {
            new peerProcess(args[0]).run();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}