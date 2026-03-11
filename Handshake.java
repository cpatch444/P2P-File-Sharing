import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

final class MessageTypes {
    public static final int CHOKE = 0;
    public static final int UNCHOKE = 1;
    public static final int INTERESTED = 2;
    public static final int NOT_INTERESTED = 3;
    public static final int HAVE = 4;
    public static final int BITFIELD = 5;
    public static final int REQUEST = 6;
    public static final int PIECE = 7;

    private MessageTypes() {}
}

public class Handshake {
    private static final String HEADER = "P2PFILESHARINGPROJ";
    private static final int HEADER_LEN = 18;
    private static final int ZERO_LEN = 10;
    private static final int PEER_ID_LEN = 4;
    public static final int HANDSHAKE_LEN = HEADER_LEN + ZERO_LEN + PEER_ID_LEN;

    private final int peerId;
    public Handshake(int peerId) {
        this.peerId = peerId;
    }
    public int getPeerId() {
        return peerId;
    }

    public byte[] encode() {
        byte[] data = new byte[HANDSHAKE_LEN];
        int index = 0;

        byte[] headerBytes = HEADER.getBytes(StandardCharsets.US_ASCII);
        for (int i = 0; i < HEADER_LEN; i++) {
            data[index] = headerBytes[i];
            index = index + 1;
        }

        for (int i = 0; i < ZERO_LEN; i++) {
            data[index] = 0;
            index++;
        }
        byte byte1 = (byte)(peerId >> 24);
        byte byte2 = (byte)(peerId >> 16);
        byte byte3 = (byte)(peerId >> 8);
        byte byte4 = (byte)peerId;
        data[index] = byte1;
        index++;
        data[index] = byte2;
        index++;
        data[index] = byte3;
        index++;
        data[index] = byte4;
        return data;
    }

    public static Handshake decode(InputStream in) throws IOException {
        byte[] data = new byte[HANDSHAKE_LEN];
        int bytes = 0;
        while (bytes < data.length) {
            int read = in.read(data, bytes, data.length - bytes);
            if (read <= 0) {
                throw new IOException("Could not read handshake");
            }
            bytes = bytes + read;
        }

        String received = new String(data, 0, HEADER_LEN, StandardCharsets.US_ASCII);
        if (HEADER.equals(received) == false) {
            throw new IOException("Header invalid: " + received);
        }
        int part1 = (data[28] & 0xFF) << 24;
        int part2 = (data[29] & 0xFF) << 16;
        int part3 = (data[30] & 0xFF) << 8;
        int part4 = (data[31] & 0xFF);
        int ID = part1 | part2 | part3 | part4;

        return new Handshake(ID);
    }
}
