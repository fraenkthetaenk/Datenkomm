package edu.hm.dako.chat.client;

import java.util.Vector;

/**
 * Interface zur Ausfuehrung von Aktionen in die Praesentationslogik
 *
 * @author Mandl
 */
public interface ChatClientUserInterface {

    /**
     * Uebergabe der Startdaten an die GUI
     *
     * @param userList Liste der aktuell angemeldeten User
     */
    public void setUserList(Vector<String> userList);

    /**
     * Uebergabe einer Nachricht zur Ausgabe in der Messagezeile
     *
     * @param sender Absender der Nachricht
     * @param message Nachrichtentext
     */
    public void setMessageLine(String sender, String message);

    /**
     * Sperren bzw. Entsperren der Eingabe von Chat-Nachrichten an der GUI
     *
     * @param block true, wenn Client warten muss, sonst false
     */
    public void setBlock(boolean block);

    /**
     * Uebergabe einer Fehlermeldung
     *
     * @param sender Absender der Fehlermeldung
     * @param errorMessage Fehlernachricht
     * @param errorCode Error Code
     */
    public void setErrorMessage(String sender, String errorMessage, long errorCode);

    /**
     * Login vollstaendig und Chat-GUI kann angezeigt werden
     */
    public void loginComplete();

    /**
     * Logout vollstaendig durchgefuehrt
     */
    public void logoutComplete();
}