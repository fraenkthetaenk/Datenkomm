package edu.hm.dako.chat.server;

/**
 * Einheitliche Schnittstelle aller Server
 */
public interface ChatServer {

    /**
     * Startet den Server
     */
    void start();

    /**
     * Stoppt den Server
     *
     * @throws Exception
     */
    void stop() throws Exception;
}
