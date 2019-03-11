package com.yq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Simple to Introduction
 * className: LogMainTest
 *
 * @author EricYang
 * @version 2019/3/7 9:14
 */
public class LogMainTest {
    private static final Logger log = LoggerFactory.getLogger(LogMainTest.class);

    public static void main(final String[] args) throws Exception {
        log.info("info");
        log.debug("debug");
        log.error("error");
    }
}
