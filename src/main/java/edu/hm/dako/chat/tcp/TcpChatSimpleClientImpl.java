package edu.hm.dako.chat.tcp;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.hm.dako.chat.client.AbstractClient;
import edu.hm.dako.chat.client.ChatClientUserInterface;
import edu.hm.dako.chat.common.ChatClientConversationStatus;
import edu.hm.dako.chat.common.ChatPDU;
import edu.hm.dako.chat.common.ExceptionHandler;
import edu.hm.dako.chat.common.SharedClientStatistics;
import edu.hm.dako.chat.connection.Connection;
import edu.hm.dako.chat.connection.ConnectionFactory;
import edu.hm.dako.chat.connection.DecoratingConnectionFactory;

/**
 * <p/>
 * Verwaltet eine Verbindung zum Server und implementiert den clientseitigen
 * Zustandsautomaten fuer das Chat-Protokoll
 *
 * @author Mandl
 */
public class TcpChatSimpleClientImpl extends AbstractClient {

	private static Log log = LogFactory.getLog(TcpChatSimpleClientImpl.class);

	private Connection connection;
	private ChatClientUserInterface userInterface;

	// Aktueller Zustand des Clients
	private ChatClientConversationStatus status;

	// Zaehler fuer gesendete Chat-Nachrichten des Clients
	private AtomicInteger messageCounter = new AtomicInteger(0);

	// Kennzeichen, ob zuletzt erwartete Chat-Response-PDU des Clients
	// angekommen ist
	private AtomicBoolean chatResponseReceived = new AtomicBoolean();

	// Zaehler fuer Logouts und empfangene Events aller Clients zum Test,
	private static AtomicInteger logoutCounter = new AtomicInteger(0);
	private static AtomicInteger eventCounter = new AtomicInteger(0);

	// Serverzeit fuer letzte Chat-Message-Bearbeitung des Clients, wird fuer
	// die Endebearbeitung genutzt
	private long lastServerTime;

	// Logout-Response-PDU, die vom Client empfangen wurde
	private ChatPDU logoutResponsePdu;

	// Thread, der die ankommenden Nachrichten fuer den Client verarbeitet
	private MessageListenerThread messageListenerThread;

	/**
	 * Konstruktor fuer Benchmarking
	 * 
	 * @param userInterface
	 * @param serverPort
	 * @param remoteServerAddress
	 * @param numberOfClient
	 * @param messageLength
	 * @param numberOfMessages
	 * @param clientThinkTime
	 * @param numberOfRetries
	 * @param responseTimeout
	 * @param sharedData
	 * @param connectionFactory
	 */
	public TcpChatSimpleClientImpl(ChatClientUserInterface userInterface,
			int serverPort, String remoteServerAddress, int numberOfClient,
			int messageLength, int numberOfMessages, int clientThinkTime,
			int numberOfRetries, int responseTimeout,
			SharedClientStatistics sharedData,
			ConnectionFactory connectionFactory) {
		super(serverPort, remoteServerAddress, numberOfClient, messageLength,
				numberOfMessages, clientThinkTime, numberOfRetries,
				responseTimeout, sharedData, connectionFactory);

		this.userInterface = userInterface;
		try {

			localPort = 0; // Port wird von Socket-Implementierung vergeben
			connection = connectionFactory.connectToServer(remoteServerAddress,
					serverPort, localPort, 20000, 20000);

			log.debug("Verbindung zum Server steht");

			// Start eines Threads zur Bearbeitung ankommender Nachrichten vom
			// Server
			messageListenerThread = new MessageListenerThread();
			messageListenerThread.start();
			messageListenerThread.setName("Client-Thread-" + clientNumber);
			log.debug("Message-Processing-Thread gestartet: "
					+ messageListenerThread.getName());
		} catch (Exception e) {
			ExceptionHandler.logException(e);
		}
	}

	/**
	 * Konstruktor fuer normale Chat-Nutzung (nicht Benchmarking)
	 * 
	 * @param userInterface
	 *            Schnittstelle zum User-Interface
	 * @param serverPort
	 *            Portnummer des Servers
	 * @param remoteServerAddress
	 *            Ip-Adresse/Hostname des Servers
	 */
	public TcpChatSimpleClientImpl(ChatClientUserInterface userInterface,
			int serverPort, String remoteServerAddress) {
		super(serverPort, remoteServerAddress);

		this.userInterface = userInterface;

		try {
			connectionFactory = getDecoratedFactory(new TcpConnectionFactory());
			connection = connectionFactory.connectToServer(remoteServerAddress,
					serverPort, localPort, 20000, 20000);
		} catch (Exception e) {
			ExceptionHandler.logException(e);
		}

		// Start eines Threads fuer die Bearbeitung ankommender Nachrichten vom
		// Server
		messageListenerThread = new MessageListenerThread();
		messageListenerThread.start();
		log.debug("Message-Listener-Thread gestartet: "
				+ messageListenerThread.getName());
	}

	/**
	 * Synchronisiertes Veraendern des Conversation-Status
	 * 
	 * @param status
	 *            Neuer Status
	 */
	private synchronized void setStatus(ChatClientConversationStatus status) {
		this.status = status;
	}

	/**
	 * Synchronisiertes Lesen des Conversation-Status
	 * 
	 * @return status
	 */
	private synchronized ChatClientConversationStatus getStatus() {
		return status;
	}

	/**
	 * Client-Thread fuer das Benchmarking: User wird beim Server registriert,
	 * alle Requests werden gesendet, Antworten werden gelesen und am Ende wird
	 * ein Logout ausgefuehrt
	 */
	@Override
	public void run() {

		try {
			// Login ausfuehren und warten, bis Server bestaetigt
			this.login(threadName);
			while (getStatus() != ChatClientConversationStatus.REGISTERED) {
				Thread.sleep(1000);
				if (getStatus() == ChatClientConversationStatus.UNREGISTERED) {
					// Fehlermeldung vom Server beim Login-Vorgang
					log.debug("User " + userName
							+ " schon im Server angemeldet");
					return;
				}
			}

			log.debug("User " + userName + " beim Server angemeldet");

			watForLoggedInClients(); // Warten, bis alle Clients eingeloggt sind

			// Alle Chat-Nachrichten senden
			int i;

			for (i = 0; i < numberOfMessagesToSend; i++) {
				sendMessageAndWaitForAck(i);
				try {
					// Zufaellige Zeit, aber maximal die angegebene Denkzeit
					// warten
					int randomThinkTime = (int) (Math.random() * clientThinkTime) + 1;
					Thread.sleep(randomThinkTime);
				} catch (Exception e) {
					ExceptionHandler.logException(e);
				}
			}

			log.debug("Gesendete Chat-Nachrichten von " + userName + ": " + i);

			// Warten, bis alle Clients bereit zum ausloggen sind (alle Clients
			// haben
			// alle Chat-Nachrichten gesendet
			waitForLoggingOutClients();

			log.debug("Anzahl gesendeter Requests: "
					+ sharedData.getNumberOfSentRequests());
			log.debug("Anzahl empfangener Responses: "
					+ sharedData.getSumOfAllReceivedMessages());
			log.debug("Anzahl empfangener Events: " + eventCounter.get());

			// Logout ausfuehren und warten, bis Server bestaetigt
			this.logout(threadName);
			while (getStatus() != ChatClientConversationStatus.UNREGISTERED) {
				Thread.sleep(1000);
			}

			postLogout(logoutResponsePdu); // Nachbearbeitung fuer die Statistik

			log.debug("User " + userName + " beim Server abgemeldet");

			connection.close();

		} catch (Exception e) {
			ExceptionHandler.logException(e);
		}
	}

	/**
	 * Ergaenzt ConnectionFactory um Logging-Funktionalitaet
	 * 
	 * @param connectionFactory
	 *            ConnectionFactory
	 * @return Dekorierte ConnectionFactory
	 */
	public static ConnectionFactory getDecoratedFactory(
			ConnectionFactory connectionFactory) {
		return new DecoratingConnectionFactory(connectionFactory);
	}

	/**
	 * Warten, bis Server einen Chat-Response als Antwort auf den letzten
	 * Chat-Request gesendet hat (nur fuer Benchmarking)
	 */

	private void waitUntilChatResponseReveiced() {

		chatResponseReceived.set(false);

		try {
			while ((!chatResponseReceived.get())) {
				log.debug(userName + " wartet auf Chat-Message-Response-PDU");
				Thread.sleep(1);
			}
		} catch (Exception e) {
			ExceptionHandler.logException(e);
		}

		return;
	}

	/**
	 * Chat-Nachricht an den Server senden und auf Antwort warten. Methode wird
	 * nur von Benchmarking-Client genutzt
	 * 
	 * @param i
	 *            Nummer des Clients
	 * @throws Exception
	 */

	private void sendMessageAndWaitForAck(int i) throws Exception {

		// Dummy-Nachricht zusammenbauen
		String chatMessage = "";
		for (int j = 0; j < messageLength; j++) {
			chatMessage += "+";
		}

		// Senden der Nachricht und warten, bis Bestaetigung vom Server da ist
		try {

			sharedData.incrSentMsgCounter(clientNumber);

			// RTT-Startzeit ermitteln
			long rttStartTime = System.nanoTime();
			tell(userName, chatMessage);

			// Warten, bis Chat-Response empfangen wurde, dann erst naechsten
			// Chat Request senden
			waitUntilChatResponseReveiced();

			// Response in Statistik aufnehmen
			long rtt = System.nanoTime() - rttStartTime;
			postReceive(i, lastServerTime, rtt);

		} catch (Exception e) {
			ExceptionHandler.logException(e);
		}
	}

	/**
	 * Chat-PDU empfangen
	 * 
	 * @return Empfangene ChatPDU
	 * @throws Exception
	 */
	private ChatPDU receive() throws Exception {
		try {
			ChatPDU receivedPdu = (ChatPDU) connection.receive();
			return receivedPdu;
		} catch (Exception e) {
			ExceptionHandler.logException(e);
		}
		return null;
	}

	/**
	 * Login-Request an den Server senden
	 * 
	 * @param name
	 *            Username (Login-Kennung)
	 * @throws Exception
	 */
	public void login(String name) throws Exception {

		userName = name;
		setStatus(ChatClientConversationStatus.REGISTERING);
		ChatPDU requestPdu = new ChatPDU();
		requestPdu.setPduType(ChatPDU.LOGIN_REQUEST);
		requestPdu.setClientStatus(getStatus());
		Thread.currentThread().setName("Client-" + userName);
		requestPdu.setClientThreadName(Thread.currentThread().getName());
		requestPdu.setUserName(userName);
		try {
			connection.send(requestPdu);
			log.debug("Login-Request-PDU fuer Client " + userName
					+ " an Server gesendet");
		} catch (Exception e) {
			ExceptionHandler.logException(e);
		}
	}

	/**
	 * Logout-Request an den Server senden
	 * 
	 * @param name
	 *            Username (Login-Kennung)
	 * @throws Exception
	 */
	public void logout(String name) throws Exception {

		setStatus(ChatClientConversationStatus.UNREGISTERING);
		ChatPDU requestPdu = new ChatPDU();
		requestPdu.setPduType(ChatPDU.LOGOUT_REQUEST);
		requestPdu.setClientStatus(getStatus());
		requestPdu.setClientThreadName(Thread.currentThread().getName());
		requestPdu.setUserName(userName);
		try {
			connection.send(requestPdu);
			log.debug("Logout-Request-PDU fuer Client " + name
					+ " an Server gesendet");

			logoutCounter.getAndIncrement();
			log.debug("Logout-Request von " + requestPdu.getUserName()
					+ " gesendet, LogoutCount = " + logoutCounter.get());

		} catch (Exception e) {
			ExceptionHandler.logException(e);
		}
	}

	/**
	 * Senden einer Chat-Nachricht zur Verteilung an den Server
	 * 
	 * @param name
	 *            Username (Login-Kennung)
	 * @param text
	 *            Chat-Nachricht
	 */
	public void tell(String name, String text) {
		ChatPDU requestPdu = new ChatPDU();
		requestPdu.setPduType(ChatPDU.CHAT_MESSAGE_REQUEST);
		requestPdu.setClientStatus(getStatus());
		requestPdu.setClientThreadName(Thread.currentThread().getName());
		requestPdu.setUserName(userName);
		requestPdu.setMessage(text);
		messageCounter.getAndIncrement();
		requestPdu.setSequenceNumber(messageCounter.get());
		
		try {
			connection.send(requestPdu);
			log.debug("Chat-Message-Request-PDU fuer Client " + name
					+ " an Server gesendet, Inhalt: " + text);
		} catch (Exception e) {

			ExceptionHandler.logException(e);
		}
	}

	/**
	 * Event vom Server zur Veraenderung der UserListe (eingeloggte Clients)
	 * verarbeiten
	 * 
	 * @param receivedPdu
	 *            Empfangene PDU
	 */
	private void handleUserListEvent(ChatPDU receivedPdu) {

		log.debug("Login- oder Logout-Event-PDU fuer Client "
				+ receivedPdu.getUserName() + " empfangen");

		// Neue Userliste zur Darstellung an User Interface uebergeben
		log.debug("Empfangene Userliste: " + receivedPdu.getClients());
		userInterface.setUserList(receivedPdu.getClients());

	}

	/*
	 * Thread wartet auf ankommende Nachrichten vom Server und bearbeitet diese.
	 */
	class MessageListenerThread extends Thread {

		boolean finished = false; // Kennzeichen zum Beenden der Bearbeitung

		/**
		 * Bearbeitung aller ankommenden Nachrichten vom Server
		 */
		public void run() {

			ChatPDU receivedPdu = null;

			while (!finished) {

				try {
					// Naechste ankommende Nachricht empfangen
					log.debug("Auf die naechste Nachricht vom Server warten");
					receivedPdu = receive();
					log.debug("Nach receive Aufruf, ankommende PDU mit PduType = "
							+ receivedPdu.getPduType());

				} catch (Exception e) {
					finished = true;
					ExceptionHandler.logException(e);
				}

				if (receivedPdu != null) {

					switch (getStatus()) {

					case REGISTERING:
						switch (receivedPdu.getPduType()) {

						case ChatPDU.LOGIN_RESPONSE:
							// Login-Bestaetigung vom Server angekommen

							if (receivedPdu.getErrorCode() == ChatPDU.LOGIN_ERROR) {
								log.debug("Login-Response-PDU fuer Client "
										+ receivedPdu.getUserName()
										+ " mit Login-Error empfangen");
								userInterface.setErrorMessage(
										receivedPdu.getUserName(),
										receivedPdu.getMessage(),
										receivedPdu.getErrorCode());
								setStatus(ChatClientConversationStatus.UNREGISTERED);
							} else {
								setStatus(ChatClientConversationStatus.REGISTERED);
								userInterface.loginComplete();
								Thread.currentThread().setName(
										"Listener" + "-" + userName);
								log.debug("Login-Response-PDU fuer Client "
										+ receivedPdu.getUserName()
										+ " empfangen");
							}
							break;

						case ChatPDU.LOGIN_EVENT:
						case ChatPDU.LOGOUT_EVENT:
							// Meldung vom Server, dass sich die Liste der
							// angemeldeten User veraendert hat
							try {
								handleUserListEvent(receivedPdu);
							} catch (Exception e) {
								ExceptionHandler.logException(e);
							}
							break;

						default:

							log.debug("Ankommende PDU im Zustand "
									+ getStatus() + " wird verworfen");
						}
						break;

					case REGISTERED:
						switch (receivedPdu.getPduType()) {
						
//						case ChatPDU.LOGOUT_RESPONSE:
//					
//						
//							// Login-Bestaetigung vom Server angekommen
//
//							setStatus(ChatClientConversationStatus.UNREGISTERED);
//							userInterface.logoutComplete();
//							userInterface.setBlock(true);
//							Thread.currentThread().setName(
//									"Listener" + "-" + userName);
//							log.debug("Logout-Response-PDU fuer Client "
//									+ receivedPdu.getUserName() + " empfangen");
//
//							break;

						case ChatPDU.LOGIN_EVENT:
						case ChatPDU.LOGOUT_EVENT:
							// Meldung vom Server, dass sich die Liste der
							// angemeldeten User veraendert hat
							try {
								handleUserListEvent(receivedPdu);
							} catch (Exception e) {
								ExceptionHandler.logException(e);
							}
							break;
						case ChatPDU.CHAT_MESSAGE_RESPONSE:
							userInterface.setBlock(false);
							break;

						case ChatPDU.CHAT_MESSAGE_EVENT:
								userInterface.setMessageLine(
								receivedPdu.getUserName(),
								receivedPdu.getMessage());
							break;

						default:
							log.debug("Ankommende PDU im Zustand "
									+ getStatus() + " wird verworfen");
						}
						break;

					case UNREGISTERING:
						switch (receivedPdu.getPduType()) {

						case ChatPDU.LOGOUT_RESPONSE:
							
							
							// Login-Bestaetigung vom Server angekommen

							setStatus(ChatClientConversationStatus.UNREGISTERED);
							userInterface.logoutComplete();
							userInterface.setBlock(true);
							Thread.currentThread().setName(
									"Listener" + "-" + userName);
							log.debug("Logout-Response-PDU fuer Client "
									+ receivedPdu.getUserName() + " empfangen");

							break;
				
							
							
						case ChatPDU.LOGIN_EVENT:
						case ChatPDU.LOGOUT_EVENT:
							// Meldung vom Server, dass sich die Liste der
							// angemeldeten User veraendert hat
							try {
								handleUserListEvent(receivedPdu);
							} catch (Exception e) {
								ExceptionHandler.logException(e);
							}
							break;

						default:
							log.debug("Ankommende PDU im Zustand "
									+ getStatus() + " wird verworfen");
							break;
						}
						break;

					case UNREGISTERED:
						log.debug("Ankommende PDU im Zustand " + getStatus()
								+ " wird verworfen");

						break;

					default:
						log.debug("Unzulaessiger Zustand " + getStatus());
					}
				}
			}
			log.debug("Ordnungsgemaesses Ende des Message-Listener-Threads");
		} // run
	}
}