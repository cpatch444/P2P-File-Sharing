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
    private final AtomicBoolean hasCompleteFile = new AtomicBoolean(false);
    private final Set<Integer> requestedPieces = ConcurrentHashMap.newKeySet();
    private final Random random = new Random();

    private final Map<String, Connection> connections = new ConcurrentHashMap<>();
    private final ChokeManager chokeManager;
    private final PrintWriter logWriter;

    private final Map<String, Boolean> peerComplete = new ConcurrentHashMap<>();
    private volatile boolean terminated = false;

    public peerProcess(String peerId) throws Exception {
        this.peerId = peerId;
        this.commonConfig = new CommonConfig("Common.cfg");
        this.peerInfoConfig = new PeerInfoConfig("PeerInfo.cfg");
        this.myInfo = peerInfoConfig.getPeerById(peerId);
        if (myInfo == null) {
            throw new IllegalArgumentException("Peer " + peerId + " not in PeerInfo.cfg");
        }

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
        int myIndex = -1;
        for (int i = 0; i < peers.size(); i++) {
            if (peers.get(i).getPeerId().equals(peerId)) {
                myIndex = i;
                break;
            }
        }
        for (int i = 0; i < myIndex; i++) {
            connectToPeer(peers.get(i));
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

    private void startConnection(Socket socket, InputStream in, OutputStream out, String remoteId) {
        Connection conn = new Connection(socket, in, out, remoteId);
        if (connections.putIfAbsent(remoteId, conn) != null) {
            try { socket.close(); } catch (IOException ignored) {}
            return;
        }
        chokeManager.addPeer(remoteId);

        // send bitfield
        conn.send(Message.bitfield(encodeBitfield()));

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
                        chokeManager.setInterested(remoteId, true);
                        log("Peer " + peerId + " received the 'interested' message from " + remoteId + ".");
                        break;
                    case MessageTypes.NOT_INTERESTED:
                        chokeManager.setInterested(remoteId, false);
                        log("Peer " + peerId + " received the 'not interested' message from " + remoteId + ".");
                        break;
                    case MessageTypes.HAVE:
                        int haveIdx = msg.getPieceIndex();
                        conn.remoteBitfield.set(haveIdx);
                        log("Peer " + peerId + " received the 'have' message from " + remoteId + " for the piece " + haveIdx + ".");
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
                        if (chokeManager.isUnchoked(remoteId) && bitfield.get(reqIdx)) {
                            byte[] piece = readPiece(reqIdx);
                            if (piece != null) conn.send(Message.piece(reqIdx, piece));
                        }
                        break;
                    case MessageTypes.PIECE:
                        int pIdx = msg.getPieceIndex();
                        byte[] content = msg.getPieceContent();
                        releaseOutstandingRequest(conn);
                        writePiece(pIdx, content);
                        bitfield.set(pIdx);
                        chokeManager.recordDownload(remoteId, 4 + content.length);
                        log("Peer " + peerId + " has downloaded the piece " + pIdx + " from " + remoteId + ". Now the number of pieces it has is " + bitfield.cardinality() + ".");
                        broadcastHave(pIdx);
                        updateInterestForAllConnections();
                        if (bitfield.cardinality() == numPieces && !hasCompleteFile.get()) {
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
            if (!bitfield.get(i) && conn.remoteBitfield.get(i)) {
                interested = true;
                break;
            }
        }
        conn.send(interested ? Message.interested() : Message.notInterested());
    }

    private void requestNextPiece(Connection conn) {
        if (!conn.unchokedByThem) return;
        if (conn.outstandingRequest != null) return;
        List<Integer> candidates = new ArrayList<>();
        for (int i = 0; i < numPieces; i++) {
            if (!bitfield.get(i) && conn.remoteBitfield.get(i) && !requestedPieces.contains(i)) {
                candidates.add(i);
            }
        }
        if (candidates.isEmpty()) return;
        int pieceIdx = candidates.get(random.nextInt(candidates.size()));
        if (requestedPieces.add(pieceIdx)) {
            conn.outstandingRequest = pieceIdx;
            conn.send(Message.request(pieceIdx));
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

    private void broadcastHave(int pieceIdx) {
        Message have = Message.have(pieceIdx);
        for (Connection c : connections.values()) {
            c.send(have);
        }
    }

    private byte[] encodeBitfield() {
        int len = (numPieces + 7) / 8;
        byte[] bf = new byte[len];
        for (int i = 0; i < numPieces; i++) {
            if (bitfield.get(i)) {
                bf[i / 8] |= (1 << (7 - (i % 8)));
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
            try (FileOutputStream fos = new FileOutputStream(path.toFile())) {
                fos.write(new byte[fileSize]);
            }
        }
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            raf.seek((long) index * pieceSize);
            raf.write(content);
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
            if (c != null) c.send(Message.unchoke());
        }
        @Override
        public void onChoke(String pid) {
            Connection c = connections.get(pid);
            if (c != null) c.send(Message.choke());
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
        volatile boolean unchokedByThem = false;
        volatile Integer outstandingRequest = null;
        private final Object sendLock = new Object();

        Connection(Socket socket, InputStream in, OutputStream out, String remotePeerId) {
            this.socket = socket;
            this.in = in;
            this.out = out;
            this.remotePeerId = remotePeerId;
        }

        void send(Message m) {
            synchronized (sendLock) {
                try {
                    m.send(out);
                } catch (IOException ignored) {}
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