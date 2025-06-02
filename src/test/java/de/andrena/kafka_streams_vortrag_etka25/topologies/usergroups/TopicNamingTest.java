package de.andrena.kafka_streams_vortrag_etka25.topologies.usergroups;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SpringBootTest
public class TopicNamingTest {

    public static final Pattern TEN_DIGIT_NUMBER_REGEX = Pattern.compile(".*[0-9]{10}.*");

    @Autowired
    private StreamsBuilder streamsBuilder;

    @Test
    void assertThatTopicNamesWereSetManually() {
        String topologyString = createTopologyString();

        System.out.println("##########################");
        System.out.println(topologyString);
        System.out.println("##########################");

        Pattern topicsRegex = Pattern.compile("\\(topic[s]?:.*\\)");
        List<String> topics = getAllMatches(topologyString, topicsRegex);

        Pattern storesRegex = Pattern.compile("\\(store[s]?:.*\\)");
        List<String> stores = getAllMatches(topologyString, storesRegex);

        SoftAssertions softAssertions = new SoftAssertions();

        for (String topic : topics) {
            softAssertions
                    .assertThat(topic)
                    .as("Topology contains topic with auto generated name: " + topic
                            + " You need to provide a fixed name, because auto generated names may change.")
                    .doesNotMatch(TEN_DIGIT_NUMBER_REGEX);
        }

        for (String storeString : stores) {
            softAssertions
                    .assertThat(storeString)
                    .as("Topology contains state store with auto generated name: " + storeString
                            + " You need to provide a fixed name, because auto generated names may change.")
                    .doesNotMatch(TEN_DIGIT_NUMBER_REGEX);
        }

        softAssertions.assertAll();
    }

    private String createTopologyString() {
        Topology topology = streamsBuilder.build();
        String topologyString = topology.describe().toString();
        return topologyString;
    }


    private List<String> getAllMatches(String str, Pattern regex) {
        List<String> matches = new ArrayList<>();
        Matcher matcher = regex.matcher(str);
        while (matcher.find()) {
            matches.add(matcher.group());
        }
        return matches;
    }
}
