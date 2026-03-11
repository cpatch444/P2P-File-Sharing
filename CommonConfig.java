import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;


public class CommonConfig {
    private String fileName;
    private int fileSize;
    private int pieceSize;
    private int neighbors;
    private int unchoking;
    private int unchokingInterval;

    public CommonConfig(String configPath) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(configPath));
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.isEmpty()) {
                continue;
            }
            String[] parts = line.split("\\s+", 2);
            if (parts.length < 2) {
                continue;
            }
            String key = parts[0];
            String value = parts[1];

            if (key.equals("FileName")) {
                fileName = value;
            } else if (key.equals("FileSize")) {
                fileSize = Integer.parseInt(value);
            } else if (key.equals("PieceSize")) {
                pieceSize = Integer.parseInt(value);
            } else if (key.equals("NumberOfPreferredNeighbors")) {
                neighbors = Integer.parseInt(value);
            } else if (key.equals("UnchokingInterval")) {
                unchoking = Integer.parseInt(value);
            } else if (key.equals("OptimisticUnchokingInterval")) {
                unchokingInterval = Integer.parseInt(value);
            } else {}
        }
    }
    public String getFileName() {
        return fileName;
        }
    public int getFileSize() {
        return fileSize;
        }
    public int getPieceSize() {
        return pieceSize;
        }
    public int getNumberOfPreferredNeighbors() {
        return neighbors;
        }
    public int getUnchokingInterval() {
        return unchoking;
        }
    public int getOptimisticUnchokingInterval() {
        return unchokingInterval;
        }
    public int getNumPieces() {
        return (fileSize + pieceSize - 1) / pieceSize;
    }
}

class PeerInfo {
    private final String peerId;
    private final String hostName;
    private final int port;
    private final boolean File;

    public PeerInfo(String peerId, String hostName, int port, boolean File) {
        this.peerId = peerId;
        this.hostName = hostName;
        this.port = port;
        this.File = File;
    }
    public String getPeerId() {
        return peerId;
        }
    public String getHostName() {
        return hostName;
        }
    public int getPort() {
        return port;
        }
    public boolean File() {
        return File;
        }
}

class PeerInfoConfig {
    private final List<PeerInfo> peers = new ArrayList<PeerInfo>();

    public PeerInfoConfig(String configPath) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(configPath));
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.isEmpty()) {
                continue;
            }
            String[] parts = line.split("\\s+");
            if (parts.length < 4) {
                continue;
            }
            String peerId = parts[0];
            String hostName = parts[1];
            String portString = parts[2];
            String fileString = parts[3];

            int port = Integer.parseInt(portString);
            int value = Integer.parseInt(fileString);
            boolean Flag = (value == 1);
            PeerInfo info = new PeerInfo(peerId, hostName, port, Flag);
            peers.add(info);
        }
    }
    public List<PeerInfo> getPeers() {
        return new ArrayList<PeerInfo>(peers);
    }

    public PeerInfo getPeerById(String peerId) {
        for (int i = 0; i < peers.size(); i++) {
            PeerInfo peer = peers.get(i);
            if (peer.getPeerId().equals(peerId)) {
                return peer;
            }
        }
        return null;
    }
}
