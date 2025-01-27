/* Copyright (c) 2013-2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Victor Olaya (Boundless) - initial implementation
 */
package org.locationtech.geogig.plumbing;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.locationtech.geogig.repository.Context;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.repository.Platform;
import org.locationtech.geogig.repository.Repository;
import org.locationtech.geogig.repository.impl.GeoGIG;
import org.locationtech.geogig.repository.impl.GlobalContextBuilder;
import org.locationtech.geogig.repository.impl.ContextBuilderImpl;
import org.locationtech.geogig.test.TestPlatform;
import org.locationtech.geogig.test.TestRepository;

/**
 *
 */
public class ParseTimestampTest extends Assert {

    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    private static final Date REFERENCE_DATE;// = new Date(1972, 10, 10, 10, 10);
    static {
        try {
            REFERENCE_DATE = format.parse("1972-10-10 10:10:10");
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public @Rule ExpectedException exception = ExpectedException.none();

    public @Rule TestName testName = new TestName();

    private ParseTimestamp command;

    private Repository repo;

    @Before
    public void setUp() throws IOException {
        URI uri = URI.create(String.format("memory://%s/%s", getClass().getSimpleName(),
                testName.getMethodName()));
        File tmp = new File(System.getProperty("java.io.tmpdir"));
        @SuppressWarnings("serial")
        Platform testPlatform = new TestPlatform(tmp, tmp) {
            public @Override long currentTimeMillis() {
                return REFERENCE_DATE.getTime();
            }
        };

        Hints hints = Hints.repository(uri).platform(testPlatform);
        Context context = GlobalContextBuilder.builder().build(hints);
        repo = new GeoGIG(context).getOrCreateRepository();
        command = repo.command(ParseTimestamp.class);
    }

    public @After void after() {
        TestRepository.closeAndDelete(repo);
    }

    @Test
    public void testWrongString() {
        command.setString("awrongstring");
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Invalid timestamp string: awrongstring");
        command.call();
        command.setString("a wrong string");
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Invalid timestamp string: a wrong string");
        command.call();
    }

    @Test
    public void testGitLikeStrings() throws ParseException {
        Date today = format.parse("1972-10-10 00:00:00");
        Date yesterday = format.parse("1972-10-09 00:00:00");
        Date aMinuteAgo = format.parse("1972-10-10 10:09:10");
        Date tenMinutesAgo = format.parse("1972-10-10 10:00:10");
        Date tenHoursTenMinutesAgo = format.parse("1972-10-10 00:00:10");
        Date aWeekAgo = format.parse("1972-10-03 10:10:10");

        Date actual;
        actual = new Date(command.setString("today").call());
        assertEquals(today, actual);
        actual = new Date(command.setString("yesterday").call());
        assertEquals(yesterday, actual);
        actual = new Date(command.setString("1.minute.ago").call());
        assertEquals(aMinuteAgo, actual);
        actual = new Date(command.setString("10.minutes.ago").call());
        assertEquals(tenMinutesAgo, actual);
        actual = new Date(command.setString("10.MINUTES.AGO").call());
        assertEquals(tenMinutesAgo, actual);
        actual = new Date(command.setString("10.hours.10.minutes.ago").call());
        assertEquals(tenHoursTenMinutesAgo, actual);
        actual = new Date(command.setString("1.week.ago").call());
        assertEquals(aWeekAgo, actual);
    }
}
