import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;


public class ChokeManager {
    public interface ChokeListener {
        void onUnchoke(String peerId);
        void onChoke(String peerId);
        void onPreferredNeighborsChanged(List<String> preferred);
        void onOptimisticNeighborChanged(String peerId);
        boolean hasCompleteFile();
    }
    private final int maxPreferred;           // k from Common.cfg
    private final ChokeListener listener;
    // peerId -> bytes downloaded from them in the current unchoking interval
    private final ConcurrentHashMap<String, AtomicLong> downloadRates = new ConcurrentHashMap<>();
    // peerId -> is this neighbor interested in our data
    private final ConcurrentHashMap<String, Boolean> interested = new ConcurrentHashMap<>();
    // Current unchoke state (true = unchoked by us)
    private final ConcurrentHashMap<String, Boolean> unchoked = new ConcurrentHashMap<>();
    // Current preferred neighbors
    private final Set<String> preferredNeighbors = Collections.synchronizedSet(new HashSet<>());
    // Current optimistically unchoked neighbor (null if none)
    private volatile String optimisticNeighbor = null;
    private final Random random = new Random();
    public ChokeManager(int maxPreferred, ChokeListener listener) {
        this.maxPreferred = maxPreferred;
        this.listener = listener;
    }
    // Call when a new peer connection is established. 
    public void addPeer(String peerId) {
        downloadRates.putIfAbsent(peerId, new AtomicLong(0));
        interested.putIfAbsent(peerId, false);
        unchoked.putIfAbsent(peerId, false);
    }
    // Call when a peer connection is lost. 
    public void removePeer(String peerId) {
        downloadRates.remove(peerId);
        interested.remove(peerId);
        unchoked.remove(peerId);
        preferredNeighbors.remove(peerId);
        if (peerId.equals(optimisticNeighbor)) {
            optimisticNeighbor = null;
        }
    }
    // State updates (called by PeerProcess as messages arrive)
    // Record that we downloaded 'bytes' from peerId during this interval. 
    public void recordDownload(String peerId, long bytes) {
        AtomicLong rate = downloadRates.get(peerId);
        if (rate != null) {
            rate.addAndGet(bytes);
        }
    }

    // Update whether peerId is interested in our data.
    public void setInterested(String peerId, boolean isInterested) {
        interested.put(peerId, isInterested);
    }

    // Returns true if peerId is currently unchoked by us.
    public boolean isUnchoked(String peerId) {
        return Boolean.TRUE.equals(unchoked.get(peerId));
    }

    // Returns current preferred neighbors (snapshot).
    public Set<String> getPreferredNeighbors() {
        return Collections.unmodifiableSet(new HashSet<>(preferredNeighbors));
    }

    // Returns current optimistically unchoked neighbor, or null.
    public String getOptimisticNeighbor() {
        return optimisticNeighbor;
    }

    //Reselects preferred neighbors. Should be called every UnchokingInterval seconds.
    public synchronized void selectPreferredNeighbors() {
        // Collect all interested neighbors
        List<String> interestedPeers = new ArrayList<>();
        for (Map.Entry<String, Boolean> entry : interested.entrySet()) {
            if (Boolean.TRUE.equals(entry.getValue())) {
                interestedPeers.add(entry.getKey());
            }
        }

        // Pick new preferred set
        List<String> newPreferred;
        if (listener.hasCompleteFile()) {
            // Random selection when we have the complete file
            newPreferred = randomSelect(interestedPeers, maxPreferred);
        } else {
            // Top-k by download rate, random tiebreak
            newPreferred = topKByRate(interestedPeers, maxPreferred);
        }

        // Reset download rates for next interval
        for (AtomicLong rate : downloadRates.values()) {
            rate.set(0);
        }

        Set<String> newSet = new HashSet<>(newPreferred);
        Set<String> oldSet = new HashSet<>(preferredNeighbors);

        // Choke peers that were preferred but no longer are (unless optimistic)
        for (String peerId : oldSet) {
            if (!newSet.contains(peerId) && !peerId.equals(optimisticNeighbor)) {
                chokePeer(peerId);
            }
        }

        // Unchoke newly preferred peers
        for (String peerId : newSet) {
            if (!oldSet.contains(peerId)) {
                unchokePeer(peerId);
            }
            // Already-preferred peers that remain preferred: no message needed
        }

        preferredNeighbors.clear();
        preferredNeighbors.addAll(newPreferred);

        listener.onPreferredNeighborsChanged(new ArrayList<>(newPreferred));
    }

    // Reselects the optimistically unchoked neighbor. Should be called every OptimisticUnchokingInterval seconds.

    public synchronized void selectOptimisticNeighbor() {
        // Candidates: choked AND interested (not a preferred neighbor)
        List<String> candidates = new ArrayList<>();
        for (Map.Entry<String, Boolean> entry : interested.entrySet()) {
            String peerId = entry.getKey();
            if (Boolean.TRUE.equals(entry.getValue())
                    && !preferredNeighbors.contains(peerId)
                    && !Boolean.TRUE.equals(unchoked.get(peerId))) {
                candidates.add(peerId);
            }
        }

        if (candidates.isEmpty()) {
            return;
        }

        String prev = optimisticNeighbor;
        String chosen = candidates.get(random.nextInt(candidates.size()));
        optimisticNeighbor = chosen;

        // Unchoke the new optimistic neighbor
        unchokePeer(chosen);

        // Re-choke the previous optimistic neighbor if it's no longer preferred
        if (prev != null && !preferredNeighbors.contains(prev) && !prev.equals(chosen)) {
            chokePeer(prev);
        }

        listener.onOptimisticNeighborChanged(chosen);
    }

    // Private helpers

    private void unchokePeer(String peerId) {
        unchoked.put(peerId, true);
        listener.onUnchoke(peerId);
    }

    private void chokePeer(String peerId) {
        unchoked.put(peerId, false);
        listener.onChoke(peerId);
    }

    // Returns up to n peers chosen randomly from the list.
    private List<String> randomSelect(List<String> peers, int n) {
        List<String> copy = new ArrayList<>(peers);
        Collections.shuffle(copy, random);
        return copy.subList(0, Math.min(n, copy.size()));
    }

     // Returns up to n peers with highest download rates.
    private List<String> topKByRate(List<String> peers, int n) {
        List<String> copy = new ArrayList<>(peers);
        // Shuffle first so equal-rate peers are in random order before stable sort
        Collections.shuffle(copy, random);
        copy.sort((a, b) -> {
            long rateA = downloadRates.getOrDefault(a, new AtomicLong(0)).get();
            long rateB = downloadRates.getOrDefault(b, new AtomicLong(0)).get();
            return Long.compare(rateB, rateA); // descending
        });
        return copy.subList(0, Math.min(n, copy.size()));
    }
}