package edu.hm.dako.chat.common;

import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.hm.dako.chat.connection.Connection;

/**
 * Eintrag in der serverseitigen Clientliste zur Verwaltung der angemeldeten User
 * inkl. des Conversation-Status.
 *
 * @author Peter Mandl
 *
 */
public class ChatClientListEntry {
	private static Log log = LogFactory.getLog(ChatClientListEntry.class);
	private String userName;                       	// Login-Name des Clients
	private Connection con;                        	// Verbindungs-Handle fuer Transportverbindung zum Client
	boolean finished; 								// Kennzeichen zum Beenden des Worker-Threads
	private long loginTime;                        	// Login-Zeitpunkt
	private long startTime;                        	// Ankunftszeit einer Chat-Message fuer die Serverzeit-Messung
	private ChatClientConversationStatus status;   	// Conversation-Status des Clients
	private long numberOfReceivedChatMessages;     	// Anzahl der verarbeiteten Chat-Nachrichten des Clients (Sequenznummer)
	private long numberOfSentEvents; 			   	// Anzahl gesendeter Event-Bestaetigungen an andere Clients
	private long numberOfReceivedEventConfirms;    	// Anzahl empfangener Event-Bestaetigungen anderer Clients
	private long numberOfLostEventConfirms;        	// Anzahl nicht erhaltener Bestaetigungen (derzeit nicht genutzt)
	private long numberOfRetries;                  	// Anzahl an Nachrichtenwiederholungen (derzeit nicht genutzt)
													// Liste, die auf alle Clients verweis, die noch kein Event-Confirm 
													// gesendet haben
	private Vector<String> waitList; 
											
	
	public ChatClientListEntry(String userName, Connection con) {
		this.userName = userName;
		this.con = con;
		finished = false;
		this.loginTime = 0;
		this.startTime = 0;
		this.status = ChatClientConversationStatus.UNREGISTERED;
		this.numberOfReceivedChatMessages = 0;
		this.numberOfSentEvents = 0;
		this.numberOfReceivedEventConfirms = 0;
		this.numberOfLostEventConfirms = 0;
		this.numberOfRetries = 0;
		this.waitList = new Vector<String>();
	}

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("ChatClientListEntry+++++++++++++++++++++++++++++++++++++++++++++");
        stringBuilder.append("UserName: " + this.userName);
        stringBuilder.append("\n");
        stringBuilder.append("Connection: " + this.con);
        stringBuilder.append("\n");
        stringBuilder.append("Status: " + this.status);
        stringBuilder.append("\n");
        stringBuilder.append("+++++++++++++++++++++++++++++++++++++++++++++ChatClientListEntry");

        return stringBuilder.toString();
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserName() {
        return userName;
    }

    public void setConnection(Connection con) {
        this.con = con;
    }

    public Connection getConnection() {
        return (con);
    }

    public void setLoginTime(long time) {
       this.loginTime = time;
    }
    
    public void setStartTime(long startTime) {
        this.startTime = startTime;
     }

    public long getLoginTime() {
        return (loginTime);
    }

    public long getStartTime() {
        return (startTime);
    }
    
    public void setNumberOfReceivedChatMessages(long nr) {
        this.numberOfReceivedChatMessages = nr;
    }

    public long getNumberOfReceivedChatMessages() {
         return (numberOfReceivedChatMessages);
    }

     public void setNumberOfSentEvents(long nr) {
         this.numberOfSentEvents = nr;
     }

     public long getNumberOfSentEvents() {
          return (numberOfSentEvents);
     }
     
     public void setNumberOfReceivedEventConfirms(long nr) {
         this.numberOfReceivedEventConfirms = nr;
     }

     public long getNumberOfReceivedEventConfirms() {
          return (numberOfReceivedEventConfirms);
     }

     public void setNumberOfLostEventConfirms(long nr) {
         this.numberOfLostEventConfirms = nr;
     }

     public long getNumberOfLostEventConfirms() {
          return (numberOfLostEventConfirms);
     }

     public void setNumberOfRetries(long nr) {
         this.numberOfRetries = nr;
     }

     public long getNumberOfRetries() {
          return (numberOfRetries);
     }
     
     public ChatClientConversationStatus getStatus() {
    	 return status;
     }

     public void setStatus(ChatClientConversationStatus status) {
    	 this.status = status;
     }
     
     public boolean isFinished() {
    	 return finished;
     }

     public void setFinished(boolean finished) {
    	 this.finished = finished;
     }


     public void incrNumberOfSentEvents() {
    	 this.numberOfSentEvents++;
     }

     public void incrNumberOfReceivedEventConfirms() {
    	 this.numberOfReceivedEventConfirms++;
     }

     public void incrNumberOfLostEventConfirms() {
    	 this.numberOfLostEventConfirms++;
     }

     public void incrNumberOfReceivedChatMessages() {
    	 this.numberOfReceivedChatMessages++;
     }

     public void incrNumberOfRetries() {
    	 this.numberOfRetries++;
     }
    	 
     public void setWaitList(Vector<String> list) {
    	 this.waitList= list;
    	 log.debug("Warteliste von " + this.userName + ": " + waitList);
     }

    public void addWaitListEntry(String userName) {
        this.waitList.add(userName);
        log.debug("Warteliste von " + this.userName + " ergaenzt um " + userName);
    }

    public Vector<String> getWaitList() {
    	 return waitList;
     }

    public void clearWaitList() {
        waitList.clear();
    }
}
