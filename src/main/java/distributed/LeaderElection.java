package distributed;

import java.io.IOException;


import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderElection implements Watcher {

	private static final String ZK_ADDRESS = "localhost:2181";
	private static final int SESSION_TIMEOUT = 3000;

	private static final Logger logger = LoggerFactory.getLogger(LeaderElection.class);

	private ZooKeeper zooKeeper;

	public static void main(String[] args) {
		LeaderElection leaderElection = new LeaderElection();
		try {
			leaderElection.connectToZookeeper();
			leaderElection.run();
			leaderElection.close();
		} catch (IOException | InterruptedException e) {
			logger.error("Error connecting to Zookeeper", e);
		}
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
				
			}else {
				synchronized (zooKeeper) {
					logger.warn("Disconnected from Zookpeer event");
					zooKeeper.notifyAll();
				}
			}

		}

	}

}
