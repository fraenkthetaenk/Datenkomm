package edu.hm.dako.chat.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.hm.dako.chat.common.ChatPDU;
import edu.hm.dako.chat.common.SharedClientStatistics;
import edu.hm.dako.chat.connection.ConnectionFactory;

/**
 * Basis fuer konkrete Client-Implementierungen.
 */
public abstract class AbstractClient implements Runnable {

	private static Log log = LogFactory.getLog(AbstractClient.class);

	protected String userName; // Username (Login-Kennung) des Clients

	protected String threadName;

	protected int clientNumber;

	protected int messageLength;

	protected int numberOfMessagesToSend;

	protected int serverPort;

	protected int localPort;

	protected String remoteServerAddress;

	protected int responseTimeout;

	protected int numberOfRetries;

	/**
	 * Denkzeit des Clients zwischen zwei Requests in ms
	 */
	protected int clientThinkTime;

	/**
	 * Gemeinsame Daten der Threads
	 */
	protected SharedClientStatistics sharedData;

	protected ConnectionFactory connectionFactory;

	/**
	 * @param serverPort             Port des Servers
	 * @param remoteServerAddress    Adresse des Servers
	 * @param clientNumber           Laufende Nummer des Test-Clients
	 * @param messageLength          Laenge einer Nachricht
	 * @param numberOfMessagesToSend Anzahl zu sendender Nachrichten je Thread
	 * @param clientThinkTime        Denkzeit des Test-Clients
	 * @param sharedData             Gemeinsame Daten der Threads
	 * @param connectionFactory      Der zu verwendende Client
	 */
	public AbstractClient(int serverPort, String remoteServerAddress, int clientNumber, int messageLength,
	                      int numberOfMessagesToSend, int clientThinkTime,
	                      int numberOfRetries, int responseTimeout,
	                      SharedClientStatistics sharedData,
	                      ConnectionFactory connectionFactory) {
		this.serverPort = serverPort;
		this.remoteServerAddress = remoteServerAddress;
		this.clientNumber = clientNumber;
		this.messageLength = messageLength;
		this.numberOfMessagesToSend = numberOfMessagesToSend;
		this.clientThinkTime = clientThinkTime;
		this.numberOfRetries = numberOfRetries;
		this.responseTimeout = responseTimeout;
		this.sharedData = sharedData;
		Thread.currentThread().setName("Client-" + String.valueOf(clientNumber + 1));
		threadName = Thread.currentThread().getName();
		this.connectionFactory = connectionFactory;
	}

	public AbstractClient(int serverPort, String remoteServerAddress) {
		this.serverPort = serverPort;
		this.remoteServerAddress = remoteServerAddress;
		this.clientNumber = 0;
		this.messageLength = 500;
		this.numberOfMessagesToSend = 0;
		this.clientThinkTime = 0;
		this.numberOfRetries = 2;
		this.responseTimeout = 1000;
		this.sharedData = null;
		Thread.currentThread().setName("Client");
		threadName = Thread.currentThread().getName();
	}


	/**
	 * Synchronisation mit allen anderen Client-Threads:
	 * Warten, bis alle Clients angemeldet sind und dann
	 * erst mit der Lasterzeugung beginnen
	 *
	 * @throws InterruptedException falls sleep unterbrochen wurde
	 */
	protected void watForLoggedInClients() throws InterruptedException {
		sharedData.getLoginSignal().countDown();
		sharedData.getLoginSignal().await();
	}

	/**
	 * Synchronisation mit allen anderen Client-Threads:
	 * Warten, bis alle Clients angemeldet sind und dann
	 * erst mit der Lasterzeugung beginnen
	 *
	 * @throws InterruptedException falls sleep unterbrochen wurde
	 */
	protected void waitForLoggingOutClients() throws InterruptedException {
		sharedData.getLogoutSignal().countDown();
		sharedData.getLogoutSignal().await();
	}
	
	/**
	 * Nacharbeit nach Empfang einer PDU vom Server
	 *
	 * @param messageNumber Fortlaufende Nachrichtennummer
	 * @param serverTime    Zeit, die der Server fuer die Bearbeitung des Chat-Message-Requests benoetigt
	 * @param rtt           Round Trip Time fuer den Request
	 */
	protected final void postReceive(int messageNumber, long serverTime, long rtt) {
		// Response-Zaehler erhoehen
		sharedData.incrReceivedMsgCounter(clientNumber, rtt, serverTime);
		log.debug(threadName + ": RTT fuer Request " + (messageNumber + 1) + ": " + rtt + " ns");
		log.debug(Thread.currentThread().getName() + ", Benoetigte Serverzeit: " + serverTime + " ns");
	}

	/**
	 * Nacharbeit nach Logout
	 *
	 * @param receivedPdu letzte empfangene PDU
	 */
	protected final void postLogout(ChatPDU receivedPdu) {
		// Zaehler fuer Statistik eintragen
		sharedData.setNumberOfSentEventMessages(clientNumber, receivedPdu.getNumberOfSentEvents());
		sharedData.setNumberOfReceivedConfirmEvents(clientNumber, receivedPdu.getNumberOfReceivedConfirms());
		sharedData.setNumberOfLostConfirmEvents(clientNumber, receivedPdu.getNumberOfLostConfirms());
		sharedData.setNumberOfRetriedEvents(clientNumber, receivedPdu.getNumberOfRetries());
		log.debug("Vom Server verarbeitete Chat-Nachrichten: " + receivedPdu.getNumberOfReceivedChatMessages());
		log.debug("Vom Server gesendete Event-Nachrichten: " + receivedPdu.getNumberOfSentEvents());
		log.debug("Dem Server bestaetigte Event-Nachrichten: " + receivedPdu.getNumberOfReceivedConfirms());
		log.debug("Im Server nicht empfangene Bestaetigungen: " + receivedPdu.getNumberOfLostConfirms());
		log.debug("Vom Server initiierte Wiederholungen: " + receivedPdu.getNumberOfRetries());
	}
}