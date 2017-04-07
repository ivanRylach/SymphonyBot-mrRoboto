package com.symphony.hackathon.bot.topusers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * </p>Created by Ivan Rylach on 4/6/17
 * @author i@symphony.com
 */
class Beacon implements Runnable {

  private final static Logger log = LoggerFactory.getLogger(Beacon.class);

  private final SymphonyService symphonyService;
  private final DataSourceService dataSourceService;

  Beacon(SymphonyService symphonyService,
      DataSourceService dataSourceService) {
    this.symphonyService = symphonyService;
    this.dataSourceService = dataSourceService;
  }

  @Override
  public void run() {
    try {
      String message = "\nMr.Roboto report ";
      DBResult dbResult = dataSourceService.getActiveUser();
      message = message + "\nThe Chattiest : " + dbResult.getName() + " - " + dbResult.getCounter();

      dbResult = dataSourceService.getActiveBot();
      message = message + "\nThe Beasty bot: " + dbResult.getName() + " - " + dbResult.getCounter();

      dbResult = dataSourceService.getBiggestBlaster();
      message = message + "\nThe Room Blaster: " + dbResult.getName() + " - " + dbResult.getCounter();

      symphonyService.sendMessage(message);
    } catch (Exception e) {
      log.error("Beacon has faded...", e);
    }
  }

}
