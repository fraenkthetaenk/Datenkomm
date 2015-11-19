package edu.hm.dako.chat.common;

import java.io.Serializable;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <p/>
 * Nachrichtenaufbau fuer Chat-Protokoll (Request, Response, Event)
 *
 * @author Mandl
 */
public class ChatPDU implements Serializable {
    private static final long serialVersionUID = -6172619032079227585L;
    private static Log log = LogFactory.getLog(ChatPDU.class);


    private int pduType; // Kommandos bzw. PDU-Typen
    								
    public final static int UNDEFINED = 0;
    public final static int LOGIN_REQUEST = 1;
    public final static int LOGIN_RESPONSE = 2; 
    public final static int LOGOUT_REQUEST = 3;
    public final static int LOGOUT_RESPONSE = 4;
    public final static int CHAT_MESSAGE_REQUEST = 5;
    public final static int CHAT_MESSAGE_RESPONSE = 6;
    public final static int CHAT_MESSAGE_EVENT = 7;
    public final static int LOGIN_EVENT = 8;  
    public final static int LOGOUT_EVENT = 9;  
    public final static int CHAT_MESSAGE_EVENT_CONFIRM = 10;
    public final static int LOGIN_EVENT_CONFIRM = 11;
    public final static int LOGOUT_EVENT_CONFIRM = 12;



    private String userName;	     	// Login-Name des Cleints
    private String eventUserName;		// Name des Clients, von dem ein Event initiiert wurde
    private String clientThreadName; 	// Name des Client-Threads, der den Request absendet
    private String serverThreadName; 	// Name des Threads, der den Request im Server bearbeitet
    private long sequenceNumber;	 	// Zaehlt die uebertragenen Nachrichten eines Clients, 
    								 	// optional nutzbar fuer unsichere Transportmechanismen
    private String message;          	// Nutzdaten (eigentliche Chat-Nachricht in Textform)
    private Vector<String> clients;	 	// Liste aller angemeldeten User
    private long serverTime; 		 	// Zeit in Nanosekunden, die der Server fuer die komplette Bearbeitung einer 
    								 	// Chat-Nachricht benoetigt (inkl. kompletter Verteilung an alle angemeldeten User) 
    								 	// Diese Zeit wird vom Server vor dem Absenden der Response eingetragen
    private ChatClientConversationStatus clientStatus; // Conversation-Status des Servers
    
    private int errorCode;				// Fehlercode, derzeit nur 1 Fehlercode definiert:
    public final static int NO_ERROR = 0; 
    public final static int LOGIN_ERROR = 1; 
     
    								 	// Daten zur die statistischen Auswertung, 
    								 	// die mit der Logout-Response-PDU mitgesendet werden:
    									// Anzahl der verarbeiteten Chat-Nachrichten des Clients
    private long numberOfReceivedChatMessages; 	
    									
    private long numberOfSentEvents;      // Anzahl an gesendeten Events an andere Clients
    private long numberOfReceivedConfirms;// Anzahl an empfangenen Bestaetigungen der anderen Clients
    private long numberOfLostConfirms;    // Anzahl verlorener bzw. nicht zugestellter Bestaetigungen andere Clients
    private long numberOfRetries;         // Anzahl der Wiederholungen von Nachrichten
    									  // (nur bei verbindungslosen Transportsystemen)
                                      

    public ChatPDU() {
    	pduType = UNDEFINED;
    	userName = null;
    	eventUserName = null;
        clientThreadName = null;
        serverThreadName = null;
        sequenceNumber = 0;
        errorCode = NO_ERROR;
        message = null;
        serverTime = 0;
        clients = null;
        clientStatus = ChatClientConversationStatus.UNREGISTERED;
        numberOfReceivedChatMessages = 0;
        numberOfSentEvents = 0;
        numberOfReceivedConfirms = 0;
        numberOfLostConfirms = 0;
        numberOfRetries = 0;
    }
    
    public ChatPDU(int cmd, Vector<String> clients) {
        this.pduType = cmd;
        this.clients = clients;
      }

    public ChatPDU(int cmd, String message) {
        this.pduType = cmd;
        this.message = message;
      }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("\n");
        stringBuilder.append("ChatPdu ****************************************************************************************************");
        stringBuilder.append("\n");
        stringBuilder.append("PduType: " + this.pduType + ", ");
        stringBuilder.append("\n");
        stringBuilder.append("userName: " + this.userName + ", ");
        stringBuilder.append("\n");
        stringBuilder.append("eventUserName: " + this.eventUserName + ", ");
        stringBuilder.append("\n");
        stringBuilder.append("clientThreadName: " + this.clientThreadName + ", ");
        stringBuilder.append("\n");
        stringBuilder.append("serverThreadName: " + this.serverThreadName + ", ");
        stringBuilder.append("\n");
        stringBuilder.append("errrorCode: " + this.errorCode + ", ");
        stringBuilder.append("\n");
        stringBuilder.append("sequenceNumber: " + this.sequenceNumber);
        stringBuilder.append("\n");
        stringBuilder.append("serverTime: " + this.serverTime + ", ");
        stringBuilder.append("\n");
        stringBuilder.append("clientStatus: " + this.clientStatus + ",");
        stringBuilder.append("\n");
        stringBuilder.append("numberOfReceivedChatMessages: " + this.numberOfReceivedChatMessages + ", ");
        stringBuilder.append("\n");
        stringBuilder.append("numberOfSentEvents: " + this.numberOfSentEvents + ", ");
        stringBuilder.append("\n");
        stringBuilder.append("numberOfLostConfirms: " + this.numberOfLostConfirms + ", ");
        stringBuilder.append("\n");
        stringBuilder.append("numberOfRetries: " + this.numberOfRetries);
        stringBuilder.append("\n");
        stringBuilder.append("clients (Userliste): " + this.clients + ", ");
        stringBuilder.append("\n");
        stringBuilder.append("message: " + this.message);
        stringBuilder.append("\n");
        stringBuilder.append("**************************************************************************************************** ChatPdu");
        stringBuilder.append("\n");
        return stringBuilder.toString();
    }

    public static void printPdu(ChatPDU pdu) {
        //System.out.println(pdu);
    	log.debug(pdu);
    }

    public void setClients(Vector<String> clients) {
        this.clients = clients;
    }
        
    public void setPduType(int pduType) {
        this.pduType = pduType;
    }
    
    public void setUserName(String userName) {
        this.userName = userName;
    }
    
    public void setEventUserName(String name) {
        this.eventUserName = name;
    }
    public void setClientThreadName(String threadName) {
        this.clientThreadName = threadName;
    }

    public void setServerThreadName(String threadName) {
        this.serverThreadName = threadName;
    }
    
    public void setMessage(String msg) {
        this.message = msg;
    }

    public void setServerTime(long time) {
        this.serverTime = time;
    }

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }
    
    public int getPduType() {
      return pduType;
    }
    
    public Vector<String> getClients() {
        return clients;
    }
    
    public String getUserName() {
        return userName;
    }
    
    public String getEventUserName() {
        return eventUserName;
    }

    public String getClientThreadName() {
        return (clientThreadName);
    }

    public String getServerThreadName() {
        return (serverThreadName);
    }

    public String getMessage() {
        return (message);
    }

    public long getServerTime() {
        return (serverTime);
    }
    
    public long getSequenceNumber() {
        return (sequenceNumber);
    }
    
    public ChatClientConversationStatus getClientStatus() {
   	 	return clientStatus;
    }
   
    public void setClientStatus(ChatClientConversationStatus clientStatus) {
   	 this.clientStatus = clientStatus;
    }
 
    public long getNumberOfSentEvents() {
        return(numberOfSentEvents);
    }

    public void setNumberOfSentEvents(long nr) {
        this.numberOfSentEvents = nr;
    }
    
    public long getNumberOfReceivedConfirms() {
        return(numberOfReceivedConfirms);
    }

    public void setNumberOfEventReceivedConfirms(long nr) {
        this.numberOfReceivedConfirms = nr;       
    }
    
    public long getNumberOfLostConfirms() {
        return(numberOfLostConfirms);
    }
    
    public void setNumberOfLostEventConfirms(long nr) {
        this.numberOfLostConfirms = nr;       
    }
    
    public long getNumberOfRetries() {
        return(numberOfRetries);
    }
    
    public void setNumberOfRetries(long nr) {
        this.numberOfRetries = nr;       
    }
    
    public long getNumberOfReceivedChatMessages() {
        return(numberOfReceivedChatMessages);
    }
    
    public void setNumberOfReceivedChatMessages(long nr) {
        this.numberOfReceivedChatMessages = nr;       
    }
    
    public long getErrorCode() {
        return(errorCode);
    }
    
    public void setErrorCode(int code) {
        this.errorCode = code;    
    }
} 