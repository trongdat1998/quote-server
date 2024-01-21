package io.bhex.broker.quote.controller.web;

import com.google.common.collect.Sets;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Set;

@RequestMapping("/internal")
@RestController
public class MetrixController {

    @GetMapping(value = "/metrics", produces = "text/plain")
    public String metrics(@RequestParam(name = "name[]", required = false) String[] names) throws IOException {
        Set<String> includedNameSet = names == null ? Collections.emptySet() : Sets.newHashSet(names);
        Enumeration<Collector.MetricFamilySamples> prometheusSamples = CollectorRegistry
            .defaultRegistry
            .filteredMetricFamilySamples((Set) includedNameSet);
        Writer writer = new StringWriter();
        TextFormat.write004(writer, prometheusSamples);
        return writer.toString();
    }

}
