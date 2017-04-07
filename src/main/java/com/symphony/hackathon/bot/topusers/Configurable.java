package com.symphony.hackathon.bot.topusers;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * </p>Created by Ivan Rylach on 4/6/17
 * @author i@symphony.com
 */
class Configurable {

  Map<String, String> initParams = new HashMap<String, String>();

  void initParams(Set<String> initParamNames) {
    for (String initParam : initParamNames) {
      String initParamValue = System.getProperty(initParam);

      if (initParamValue == null) {
        throw new IllegalArgumentException(
            "Cannot find required property; make sure you're using -D" + initParam
                + " to run HelloWorldBot");
      } else {
        initParams.put(initParam, initParamValue);
      }
    }
  }

}
