package distributed;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderElection implements Watcher {

	private static final String ZK_ADDRESS = "localhost:2181";
	private static final int SESSION_TIMEOUT = 3000;
	private static final String ELECTION_NAMESPACE = "/election";	
		
	private static final Logger logger = LoggerFactory.getLogger(LeaderElection.class);
	private ZooKeeper zooKeeper;
	
	private String currentZnoneName;

	public static void main(String[] args) {
		LeaderElection leaderElection = new LeaderElection();
		try {
			leaderElection.connectToZookeeper();
			leaderElection.volunteerForLeadership();
			leaderElection.electLeader();
			leaderElection.run();
			leaderElection.close();
		} catch (IOException | InterruptedException | KeeperException e) {
			logger.error("Error", e);
		}
	}
	
	public void volunteerForLeadership() throws KeeperException, InterruptedException {
		String znodePrefix = ELECTION_NAMESPACE + "/c_";
		String znodeFullPath = zooKeeper.create(znodePrefix, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		logger.warn("znode name {}", znodeFullPath);
		this.currentZnoneName = znodeFullPath.replace("/election/", "");
	}
	
	public void electLeader() throws KeeperException, InterruptedException {
		
		List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
		Collections.sort(children);
		
		String smallestChild = children.get(0);
		
		if(smallestChild.equals(currentZnoneName)) {
			logger.warn("I am the leader {}", currentZnoneName);
			return;
		}
		
		logger.warn("I am NOT the leader, {} is the leader", smallestChild);
		
	}

	public void connectToZookeeper() throws IOException {
		this.zooKeeper = new ZooKeeper(ZK_ADDRESS, SESSION_TIMEOUT, this);
	}

	public void run() throws InterruptedException {
		synchronized (zooKeeper) {
			zooKeeper.wait();
		}
	}
	
	public void close() throws InterruptedException {
		zooKeeper.close();
	}
	
	/**
	 * This will called by the Zookeeper library on a separated thread when a new
	 * event come from Zookeeper service
	 * 
	 * Generally connect events don't have any type, when we get a none type we
	 * gonna check the state, and SyncConnected tell us we successfully connected to
	 * Zoopkeper service
	 * 
	 */
	@Override
	public void process(WatchedEvent event) {

		switch (event.getType()) {
		case None:
			if (event.getState() == Event.KeeperState.SyncConnected) {
				logger.warn("Successfuly connected to Zookeeper");

			} else if(event.getState() == Event.KeeperState.Disconnected) {
				synchronized (zooKeeper) {
					logger.warn("Disconnected from Zookpeer event");
					zooKeeper.notifyAll();
				}
			}

		}

	}

}
