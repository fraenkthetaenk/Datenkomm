package edu.hm.dako.chat.common;

public enum ChatClientConversationStatus  {

	  UNREGISTERED, 	// Client nicht eingeloggt
	  REGISTERING,     	// Client-Login in Arbeit
	  REGISTERED,      	// Client eingeloggt
	  UNREGISTERING;   	// Client-Logout in Arbeit
	  
}
