package com.symphony.hackathon.bot.topusers;

/**
 * </p>Created by Ivan Rylach on 4/6/17
 * @author i@symphony.com
 */
public class DBResult {

  private long userId;
  private long counter;
  private String name;

  public DBResult(long userId, long counter, String name) {
    this.userId = userId;
    this.counter = counter;
    this.name = name;
  }

  public long getUserId() {
    return userId;
  }

  public long getCounter() {
    return counter;
  }

  public String getName() {
    return name;
  }
}
