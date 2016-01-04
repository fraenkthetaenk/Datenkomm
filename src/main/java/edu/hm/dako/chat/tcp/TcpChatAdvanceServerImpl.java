package edu.hm.dako.chat.tcp;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.hm.dako.chat.common.ExceptionHandler;
import edu.hm.dako.chat.common.ChatClientConversationStatus;
import edu.hm.dako.chat.common.ChatClientListEntry;
import edu.hm.dako.chat.common.ChatPDU;
import edu.hm.dako.chat.common.SharedChatClientList;
import edu.hm.dako.chat.connection.Connection;
import edu.hm.dako.chat.connection.ServerSocket;
import edu.hm.dako.chat.server.ChatServer;

/**
 * <p/>
 * Chat-Server-Implementierung
 *
 * @author Mandl
 */
public class TcpChatAdvanceServerImpl implements ChatServer {

	private static Log log = LogFactory.getLog(TcpChatAdvanceServerImpl.class);

	private ChatPDU SaveWaitingListPDU;
	private Connection saveConnection;

	// Threadpool fuer Woekerthreads
	private final ExecutorService executorService;

	// Socket fuer den Listener, der alle Verbindungsaufbauwuensche der Clients
	// entgegennimmt
	private ServerSocket socket;

	// Gemeinsam fuer alle Workerthreads verwaltete Liste aller eingeloggten
	// Clients
	private SharedChatClientList clients;

	// Startzeit fuer die RTT-Messung der Request-Bearbeitungsdauer eines
	// Clients
	private long startTime;

	// Zaehler fuer Logouts und gesendete Events nur zum Tests
	private static AtomicInteger logoutCounter = new AtomicInteger(0);
	private static AtomicInteger eventCounter = new AtomicInteger(0);

	public TcpChatAdvanceServerImpl(ExecutorService executorService,
			ServerSocket socket) {
		log.debug("TcpChatAdvanceServerImpl konstruiert");
		this.executorService = executorService;
		this.socket = socket;
	}

	@Override
	public void start() {

		clients = SharedChatClientList.getInstance(); // Clientliste erzeugen
		while (!Thread.currentThread().isInterrupted() && !socket.isClosed()) {
			try {
				// Auf ankommende Verbindungsaufbauwuensche warten
				System.out
						.println("SimpleChatServer wartet auf Verbindungsanfragen von Clients...");
				Connection connection = socket.accept();
				log.debug("Neuer Verbindungsaufbauwunsch empfangen");

				// Neuen Workerthread starten
				executorService.submit(new ChatWorker(connection));
			} catch (Exception e) {
				log.error("Exception beim Entgegennehmen von Verbindungsaufbauwuenschen: "
						+ e);
				ExceptionHandler.logException(e);
			}
		}
	}

	@Override
	public void stop() throws Exception {
		System.out.println("SimpleChatServer beendet sich");
		clients.deleteAll(); // Loeschen der Userliste
		Thread.currentThread().interrupt();
		socket.close();
		log.debug("Listen-Socket geschlossen");
		executorService.shutdown();
		try {
			executorService.awaitTermination(10, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			log.error("Das Beenden des ExecutorService wurde unterbrochen");
			ExceptionHandler.logExceptionAndTerminate(e);
		}
		log.debug("Threadpool freigegeben");
	}

	/**
	 * Worker-Thread zur serverseitigen Bedienung einer Session mit einem
	 * Client. Jedem Client wird serverseitig ein Worker-Thread zugeordnet.
	 * 
	 * @author Mandl
	 *
	 */
	private class ChatWorker implements Runnable {

		private Connection connection; // Verbindungs-Handle
		private boolean finished = false;
		private String userName; // Username des durch den Worker-Thread
									// bedienten Clients
		// private ChatPDU SaveWaitingListPDU;
		//private Connection saveConnection;

		private ChatWorker(Connection con) {
			this.connection = con;
		}

		@Override
		public void run() {

			log.debug("ChatWorker-Thread erzeugt, Threadname: "
					+ Thread.currentThread().getName());
			while (!finished && !Thread.currentThread().isInterrupted()// &&
																		// waitingForConfirm==false
			) {
				try {
					// Warte auf naechste Nachricht des Clients und fuehre
					// entsprechende Aktion aus
					handleIncomingMessage();
				} catch (Exception e) {
					log.error("Exception waehrend der Nachrichtenverarbeitung");
					ExceptionHandler.logException(e);
				}
			}
			log.debug(Thread.currentThread().getName() + " beendet sich");
			closeConnection();
		}

		/**
		 * Erzeugen einer Logout-Event-PDU
		 * 
		 * @param receivedPdu
		 *            Empfangene PDU (Logout-Request-PDU)
		 * @return Erzeugte PDU
		 */
		private ChatPDU createLogoutEventPdu(ChatPDU receivedPdu) {

			ChatPDU pdu = new ChatPDU();
			pdu.setPduType(ChatPDU.LOGOUT_EVENT);
			pdu.setUserName(userName);
			pdu.setEventUserName(userName);
			pdu.setServerThreadName(Thread.currentThread().getName());
			pdu.setClientThreadName(receivedPdu.getClientThreadName());
			pdu.setClientStatus(ChatClientConversationStatus.UNREGISTERING);
			return pdu;
		}

		/**
		 * Erzeugen einer Login-Event-PDU
		 * 
		 * @param receivedPdu
		 *            Empfangene PDU (Login-Request-PDU)
		 * @return Erzeugte PDU
		 */
		private ChatPDU createLoginEventPdu(ChatPDU receivedPdu) {

			ChatPDU pdu = new ChatPDU();
			pdu.setPduType(ChatPDU.LOGIN_EVENT);
			pdu.setServerThreadName(Thread.currentThread().getName());
			pdu.setClientThreadName(receivedPdu.getClientThreadName());
			pdu.setUserName(userName);
			pdu.setEventUserName(receivedPdu.getUserName());
			pdu.setUserName(receivedPdu.getUserName());
			pdu.setClientStatus(ChatClientConversationStatus.REGISTERING);
			return pdu;
		}

		/**
		 * Erzeugen einer Login-Response-PDU
		 * 
		 * @param receivedPdu
		 *            Empfangene PDU (Login-Request-PDU)
		 * @return Erzeugte PDU
		 */
		private ChatPDU createLoginResponsePdu(ChatPDU receivedPdu) {

			ChatPDU pdu = new ChatPDU();
			pdu.setPduType(ChatPDU.LOGIN_RESPONSE);
			pdu.setServerThreadName(Thread.currentThread().getName());
			pdu.setClientThreadName(receivedPdu.getClientThreadName());
			pdu.setUserName(receivedPdu.getUserName());

			ChatClientListEntry client = clients.getClient(receivedPdu
					.getUserName());
			if (client != null) {
				pdu.setClientStatus(client.getStatus());
			} else {
				pdu.setClientStatus(ChatClientConversationStatus.REGISTERED);
			}
			return pdu;
		}

		/**
		 * Erzeugen einer Chat-Message-Event-PDU
		 * 
		 * @param receivedPdu
		 *            (Chat-Message-Request-PDU)
		 * @return Erzeugte PDU
		 */
		private ChatPDU createChatMessageEventPdu(ChatPDU receivedPdu) {

			ChatPDU pdu = new ChatPDU();
			pdu.setPduType(ChatPDU.CHAT_MESSAGE_EVENT);
			pdu.setServerThreadName(Thread.currentThread().getName());
			pdu.setClientThreadName(receivedPdu.getClientThreadName());
			pdu.setUserName(userName);
			pdu.setEventUserName(receivedPdu.getUserName());
			pdu.setSequenceNumber(receivedPdu.getSequenceNumber());
			pdu.setClientStatus(ChatClientConversationStatus.REGISTERED);
			pdu.setMessage(receivedPdu.getMessage());
			return pdu;
		}

		/**
		 * Erzeugen einer Chat-Message-Response-PDU
		 * 
		 * @param receivedPdu
		 *            (Chat-Message-Request-PDU)
		 * @return Erzeugte PDU
		 */
		private ChatPDU createChatMessageResponsePdu(ChatPDU receivedPdu) {

			ChatPDU pdu = new ChatPDU();
			pdu.setPduType(ChatPDU.CHAT_MESSAGE_RESPONSE);
			pdu.setServerThreadName(Thread.currentThread().getName());
			pdu.setClientThreadName(receivedPdu.getClientThreadName());
			pdu.setEventUserName(receivedPdu.getEventUserName());
			pdu.setUserName(receivedPdu.getUserName());
			pdu.setClientStatus(ChatClientConversationStatus.REGISTERED);
			ChatClientListEntry client = clients.getClient(receivedPdu
					.getUserName());

			if (client != null) {
				pdu.setClientStatus(client.getStatus());
				pdu.setServerTime(System.nanoTime() - client.getStartTime());
				pdu.setSequenceNumber(client.getNumberOfReceivedChatMessages());
				pdu.setNumberOfSentEvents(client.getNumberOfSentEvents());
				pdu.setNumberOfLostEventConfirms(client
						.getNumberOfLostEventConfirms());
				pdu.setNumberOfEventReceivedConfirms(client
						.getNumberOfReceivedEventConfirms());
				pdu.setNumberOfRetries(client.getNumberOfRetries());
				pdu.setNumberOfReceivedChatMessages(client
						.getNumberOfReceivedChatMessages());
			}
			return pdu;
		}

		/**
		 * Erzeugen einer Logout-Response-PDU
		 * 
		 * @param pdu
		 *            Empfangene PDU
		 * @return Erzeugte PDU
		 */
		private ChatPDU createLogoutResponsePdu(ChatPDU receivedPdu) {

			ChatPDU pdu = new ChatPDU();
			pdu.setPduType(ChatPDU.LOGOUT_RESPONSE);
			pdu.setServerThreadName(Thread.currentThread().getName());
			pdu.setClientThreadName(receivedPdu.getClientThreadName());
			pdu.setUserName(receivedPdu.getUserName());
			pdu.setClientStatus(ChatClientConversationStatus.UNREGISTERED);

			ChatClientListEntry client = clients.getClient(receivedPdu
					.getUserName());
			if (client != null) {
				pdu.setClientStatus(client.getStatus());
				pdu.setNumberOfSentEvents(client.getNumberOfSentEvents());
				pdu.setNumberOfLostEventConfirms(client
						.getNumberOfLostEventConfirms());
				pdu.setNumberOfEventReceivedConfirms(client
						.getNumberOfReceivedEventConfirms());
				pdu.setNumberOfRetries(client.getNumberOfRetries());
				pdu.setNumberOfReceivedChatMessages(client
						.getNumberOfReceivedChatMessages());
			}
			return pdu;
		}

		/**
		 * Erzeugen einer Login-Response-PDU mit Fehlermeldung
		 * 
		 * @param pdu
		 *            Empfangene PDU
		 * @return Erzeugte PDU
		 */
		private ChatPDU createLoginErrorResponsePdu(ChatPDU receivedPdu,
				int errorCode) {

			ChatPDU pdu = new ChatPDU();
			pdu.setPduType(ChatPDU.LOGIN_RESPONSE);
			pdu.setServerThreadName(Thread.currentThread().getName());
			pdu.setClientThreadName(receivedPdu.getClientThreadName());
			pdu.setUserName(receivedPdu.getUserName());
			pdu.setClientStatus(ChatClientConversationStatus.UNREGISTERED);
			pdu.setErrorCode(errorCode);
			return pdu;
		}

		/**
		 * Senden eines Login-List-Update-Event an alle angemeldeten Clients
		 * 
		 * @param pdu
		 *            Zu sendende PDU
		 */
		private void sendLoginListUpdateEvent(ChatPDU pdu) {

			// Liste der eingeloggten User ermitteln
			Vector<String> clientList = clients.getClientNameList();

			// Beim Logout-Event den Client, der sich abmeldet, ausschließen
			if (pdu.getPduType() == ChatPDU.LOGOUT_EVENT) {
				clientList.remove(pdu.getEventUserName());
			}

			log.debug("Aktuelle Clientliste: " + clientList);

			pdu.setClients(clientList);

			for (String s : new Vector<String>(clientList)) {
				log.debug("Fuer "
						+ s
						+ " wird Login- oder Logout-Event-PDU an alle aktiven Clients gesendet");

				ChatClientListEntry client = clients.getClient(s);
				try {
					if (client != null) {

						client.getConnection().send(pdu);
						log.debug("Login- oder Logout-Event-PDU an "
								+ client.getUserName() + " gesendet");
					}
				} catch (Exception e) {
					log.debug("Senden einer Login- oder Logout-Event-PDU an "
							+ s + " nicht moeglich");
					ExceptionHandler.logException(e);
				}
			}

		}

		/**
		 * Login-Request bearbeiten: Neuen Client anlegen, alle Clients
		 * informieren, Response senden
		 * 
		 * @param receivedPdu
		 *            Empfangene PDU
		 * @param con
		 *            Verbindung zum neuen Client
		 */
		private void login(ChatPDU receivedPdu, Connection con) {

			ChatPDU pdu;

			if (!clients.existsClient(receivedPdu.getUserName())) {

				log.debug("User nicht in Clientliste: "
						+ receivedPdu.getUserName());
				ChatClientListEntry client = new ChatClientListEntry(
						receivedPdu.getUserName(), con);
				client.setLoginTime(System.nanoTime());
				clients.createClient(receivedPdu.getUserName(), client);
				clients.changeClientStatus(receivedPdu.getUserName(),
						ChatClientConversationStatus.REGISTERING);
				log.debug("User " + receivedPdu.getUserName()
						+ " nun in Clientliste");
				userName = receivedPdu.getUserName();
				Thread.currentThread().setName(receivedPdu.getUserName());
				log.debug("Laenge der Clientliste: " + clients.size());

				clients.createWaitList(receivedPdu.getUserName());
				// Login-Event an alle Clients (auch an den gerade aktuell
				// anfragenden) senden
				SaveWaitingListPDU = receivedPdu;
				saveConnection = con;
				pdu = createLoginEventPdu(receivedPdu);
				pdu.setMessage(clients.getWaitListSize(userName) + "");
				sendLoginListUpdateEvent(pdu);

			} else {
				// User bereits angemeldet, Fehlermeldung an Client senden,
				// Fehlercode an Client senden
				pdu = createLoginErrorResponsePdu(receivedPdu,
						ChatPDU.LOGIN_ERROR);

				try {
					con.send(pdu);
					log.debug("Login-Response-PDU an "
							+ receivedPdu.getUserName() + " mit Fehlercode "
							+ ChatPDU.LOGIN_ERROR + " gesendet");
				} catch (Exception e) {
					log.debug("Senden einer Login-Response-PDU an "
							+ receivedPdu.getUserName() + " nicth moeglich");
					ExceptionHandler.logExceptionAndTerminate(e);
				}
			}
		}

		private void logout(ChatPDU receivedPdu, Connection con) {
			ChatPDU pdu;
			if (clients.existsClient(receivedPdu.getUserName())) {

				clients.createWaitList(receivedPdu.getUserName());
				// try {
				// clients.deleteWaitListEntry(receivedPdu.getUserName(),
				// receivedPdu.getUserName());
				// } catch (Exception e) {
				// // TODO Auto-generated catch block
				// e.printStackTrace();
				// }
				// Login-Event an alle Clients (auch an den gerade aktuell
				// anfragenden) senden
				SaveWaitingListPDU = receivedPdu;
				saveConnection = con;
				pdu = createLogoutEventPdu(receivedPdu);
				sendLoginListUpdateEvent(pdu);
				
				try {
					clients.getClient(SaveWaitingListPDU.getUserName()).getConnection().send(pdu);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				log.debug("User in Clientliste: " + receivedPdu.getUserName());
				clients.changeClientStatus(receivedPdu.getUserName(),
						ChatClientConversationStatus.UNREGISTERING);

				log.debug("User " + receivedPdu.getUserName()
						+ " nun nicht mehr in Clientliste");
				log.debug("Laenge der Clientliste: " + clients.size());
			}
		}

		/**
		 * Verbindung zu einem Client ordentlich abbauen
		 */
		private void closeConnection() {

			log.debug("Schliessen der Chat-Connection zum Client " + userName);

			// Bereinigen der Clientliste falls erforderlich

			if (clients.existsClient(userName)) {
				clients.finish(userName);
				log.debug("Close Connection fuer " + userName
						+ ", Laenge der Clientliste vor deleteClient: "
						+ clients.size());
				clients.deleteClient(userName);
				log.debug("Laenge der Clientliste nach deleteClient fuer: "
						+ userName + ": " + clients.size());
			}

			try {
				connection.close();
			} catch (Exception e) {
				ExceptionHandler.logException(e);
			}
		}

		/**
		 * Verarbeitung einer ankommenden Nachricht eines Clients
		 * (Zustandsautomat des Servers)
		 * 
		 * @throws Exception
		 */
		private void handleIncomingMessage() throws Exception {

			// Warten auf naechste Nachricht

			ChatPDU receivedPdu;
			try {

				receivedPdu = (ChatPDU) connection.receive();
				startTime = System.nanoTime(); // Zeitmessung fuer RTT starten
			} catch (Exception e) {
				log.error("Empfang einer Nachricht fehlgeschlagen, Workerthread fuer User: "
						+ userName);
				finished = true;
				ExceptionHandler.logException(e);
				return;
			}

			// Empfangene Nachricht bearbeiten
			try {
				switch (receivedPdu.getPduType()) {

				case ChatPDU.LOGIN_REQUEST:
					// Neuer Client moechte sich einloggen, Client in
					// Client-Liste eintragen
					log.debug("Login-Request-PDU fuer "
							+ receivedPdu.getUserName() + " empfangen");
					login(receivedPdu, connection);
					System.out.println("sie sind im login request" + "||||"
							+ "        " + SaveWaitingListPDU.getUserName());

					break;

				// DIeser Case ist seler geschrieben und evt. nicht nötig
				case ChatPDU.LOGOUT_REQUEST:
					log.debug("Logout-Request-PDU fuer "
							+ receivedPdu.getUserName() + " empfangen");
					logout(receivedPdu, connection);
					System.out.println("sie sind im logout request" + "||||"
							+ "        " + SaveWaitingListPDU.getUserName());

					break;

				case ChatPDU.CHAT_MESSAGE_REQUEST:
					clients.createWaitList(receivedPdu.getUserName());
					try {
						clients.deleteWaitListEntry(receivedPdu.getUserName(),
								receivedPdu.getUserName());
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					SaveWaitingListPDU = receivedPdu;
					saveConnection = connection;

					ChatPDU pdu;

					pdu = createChatMessageEventPdu(receivedPdu);
					sendMEssageUpdatePdu(pdu);

					break;

				case ChatPDU.CHAT_MESSAGE_EVENT_CONFIRM:
					clients.deleteWaitListEntry(
							SaveWaitingListPDU.getUserName(), userName);
					if (clients.getWaitListSize(SaveWaitingListPDU
							.getUserName()) == 0) {
						clients.deleteWaitList(SaveWaitingListPDU.getUserName());
						pdu = createLoginResponsePdu(SaveWaitingListPDU);

						try {
							if (clients.getClient(SaveWaitingListPDU
									.getUserName()) != null) {
								saveConnection.send(pdu);
								log.debug("Login-Response-PDU an "
										+ SaveWaitingListPDU.getUserName()
										+ " gesendet");
							}
						} catch (Exception e) {
							log.error("Senden einer Login-Response-PDU an "
									+ SaveWaitingListPDU.getUserName()
									+ " nicht moeglich");
							ExceptionHandler.logException(e);
						}
						pdu = createChatMessageResponsePdu(SaveWaitingListPDU);
						sendMEssageUpdatePdu(SaveWaitingListPDU);

					}
					break;

				case ChatPDU.LOGIN_EVENT_CONFIRM:

					clients.deleteWaitListEntry(
							SaveWaitingListPDU.getUserName(), userName);
					System.out.println("sie sind im login cpnfirm" + "||||"
							+ "        " + SaveWaitingListPDU.getUserName());
					System.out.println(clients
							.getWaitListSize(SaveWaitingListPDU.getUserName()));
					if (clients.getWaitListSize(SaveWaitingListPDU
							.getUserName()) == 0) {
						clients.deleteWaitList(SaveWaitingListPDU.getUserName());
						pdu = createLoginResponsePdu(SaveWaitingListPDU);

						try {
							if (clients.getClient(SaveWaitingListPDU
									.getUserName()) != null) {
								saveConnection.send(pdu);
								log.debug("Login-Response-PDU an "
										+ SaveWaitingListPDU.getUserName()
										+ " gesendet");
							}
						} catch (Exception e) {
							log.error("Senden einer Login-Response-PDU an "
									+ SaveWaitingListPDU.getUserName()
									+ " nicht moeglich");
							ExceptionHandler.logException(e);
						}

						clients.changeClientStatus(
								SaveWaitingListPDU.getUserName(),
								ChatClientConversationStatus.REGISTERED);

					}

					break;

				case ChatPDU.LOGOUT_EVENT_CONFIRM:
					clients.deleteWaitListEntry(
							SaveWaitingListPDU.getUserName(), userName);
					System.out.println("sie sind im logout cpnfirm" + "||||"
							+ "        " + SaveWaitingListPDU.getUserName());
					System.out.println(clients
							.getWaitListSize(SaveWaitingListPDU.getUserName()));
					if (clients.getWaitListSize(SaveWaitingListPDU
							.getUserName()) == 0) {
						clients.deleteWaitList(SaveWaitingListPDU.getUserName());

						pdu = createLogoutResponsePdu(SaveWaitingListPDU);

						try {
							if (clients.getClient(SaveWaitingListPDU
									.getUserName()) != null) {
								saveConnection.send(pdu);
								log.debug("Logout-Response-PDU an "
										+ SaveWaitingListPDU.getUserName()
										+ " gesendet");
							}
						} catch (Exception e) {
							log.error("Senden einer Logout-Response-PDU an "
									+ SaveWaitingListPDU.getUserName()
									+ " nicht moeglich");
							ExceptionHandler.logException(e);
						}

						clients.changeClientStatus(
								SaveWaitingListPDU.getUserName(),
								ChatClientConversationStatus.UNREGISTERED);
						clients.deleteClient(SaveWaitingListPDU.getUserName());

					}
					break;

				default:
					log.debug("Falsche PDU empfangen von Client: "
							+ receivedPdu.getUserName() + ", PduType: "
							+ receivedPdu.getPduType());
					break;
				}
			} catch (Exception e) {
				log.error("Exception bei der Nachrichtenverarbeitung");
				ExceptionHandler.logExceptionAndTerminate(e);
			}
		}

		private void sendMEssageUpdatePdu(ChatPDU pdu) {

			// Liste der eingeloggten User ermitteln
			Vector<String> clientList = clients.getClientNameList();

			// Beim Logout-Event den Client, der sich abmeldet, ausschließen

			// log.debug("Aktuelle Clientliste: " + clientList);

			pdu.setClients(clientList);

			for (String s : new Vector<String>(clientList)) {
				// log.debug("Fuer " + s +
				// " wird Login- oder Logout-Event-PDU an alle aktiven Clients gesendet"
				// );

				ChatClientListEntry client = clients.getClient(s);
				try {
					if (client != null) {

						client.getConnection().send(pdu);
						// log.debug("Login- oder Logout-Event-PDU an " +
						// client.getUserName() + " gesendet");
					}
				} catch (Exception e) {
					// log.debug("Senden einer Login- oder Logout-Event-PDU an "
					// + s + " nicht moeglich");
					ExceptionHandler.logException(e);
				}
			}
		}
	}
}
