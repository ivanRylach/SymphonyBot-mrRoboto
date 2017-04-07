package com.symphony.hackathon.bot.topusers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.symphonyoss.client.SymphonyClient;
import org.symphonyoss.client.SymphonyClientFactory;
import org.symphonyoss.client.model.Chat;
import org.symphonyoss.client.model.SymAuth;
import org.symphonyoss.exceptions.AuthorizationException;
import org.symphonyoss.exceptions.InitException;
import org.symphonyoss.exceptions.SymException;
import org.symphonyoss.symphony.clients.AuthorizationClient;
import org.symphonyoss.symphony.clients.model.SymMessage;
import org.symphonyoss.symphony.clients.model.SymUser;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * </p>Created by Ivan Rylach on 4/6/17
 * @author i@symphony.com
 */
class SymphonyService extends Configurable {

  private final static Logger log = LoggerFactory.getLogger(SymphonyService.class);
  private static Set<String> initParamNames = new HashSet<>();

  static {
    initParamNames.add("sessionauth.url");
    initParamNames.add("keyauth.url");
    initParamNames.add("pod.url");
    initParamNames.add("agent.url");
    initParamNames.add("truststore.file");
    initParamNames.add("truststore.password");
    initParamNames.add("bot.user.cert.file");
    initParamNames.add("bot.user.cert.password");
    initParamNames.add("bot.user.email");
    initParamNames.add("receiver.user.email");
  }

  private SymphonyClient symphonyClient;
  private Chat chat;

  SymphonyService() throws SymException {
    initParams(initParamNames);
    initAuth();
    initChat();
  }

  void shutdown() {
    try {
      this.symphonyClient.shutdown();
    } catch (Exception e) {
      log.error("Failed to shutdown gracefully...", e);
    }
  }

  void sendMessage(String message)
      throws SymException {
    SymMessage messageSubmission = new SymMessage();
    messageSubmission.setFormat(SymMessage.Format.TEXT);
    messageSubmission.setMessage(message);

    symphonyClient.getMessageService().sendMessage(chat, messageSubmission);
  }

  private void initAuth()
      throws AuthorizationException, InitException {

    this.symphonyClient = SymphonyClientFactory.getClient(SymphonyClientFactory.TYPE.BASIC);

    log.debug("{} {}", System.getProperty("sessionauth.url"),
        System.getProperty("keyauth.url"));

    AuthorizationClient authClient = new AuthorizationClient(
        initParams.get("sessionauth.url"),
        initParams.get("keyauth.url"));

    authClient.setKeystores(
        initParams.get("truststore.file"),
        initParams.get("truststore.password"),
        initParams.get("bot.user.cert.file"),
        initParams.get("bot.user.cert.password"));

    SymAuth symAuth = authClient.authenticate();

    this.symphonyClient.init(
        symAuth,
        initParams.get("bot.user.email"),
        initParams.get("agent.url"),
        initParams.get("pod.url")
    );
  }

  private void initChat()
      throws SymException {
    this.chat = new Chat();
    chat.setLocalUser(symphonyClient.getLocalUser());
    Set<SymUser> remoteUsers = new HashSet<>();
    log.info("initChat#remoteUsers: " + Arrays.toString(remoteUsers.toArray()));

    remoteUsers.add(
        symphonyClient.getUsersClient().getUserFromEmail(initParams.get("receiver.user.email")));
    chat.setRemoteUsers(remoteUsers);
    chat.setStream(symphonyClient.getStreamsClient().getStream(remoteUsers));
  }
}
