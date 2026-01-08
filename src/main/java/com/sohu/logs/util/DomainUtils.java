package com.sohu.logs.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class DomainUtils {
    private static final Pattern URL_DOMAIN_PATTERN = Pattern.compile("^(?:https?://)?(?:www\\.)?([^/]+)");
    
    public static String extractDomain(String url) {
        if (url == null || url.trim().isEmpty()) {
            return "";
        }
        
        try {
            String urlToParse = url.trim();
            if (!urlToParse.contains("://")) {
                urlToParse = "http://" + urlToParse;
            }
            
            String host = new URI(urlToParse).getHost();
            if (host == null) {
                return extractSimpleDomain(url);
            }
            
            String[] parts = host.split("\\.");
            if (parts.length >= 2) {
                return parts[parts.length - 2];
            }
            return host;
        } catch (URISyntaxException e) {
            return extractSimpleDomain(url);
        }
    }
    
    private static String extractSimpleDomain(String url) {
        Matcher matcher = URL_DOMAIN_PATTERN.matcher(url);
        if (matcher.find()) {
            String domain = matcher.group(1);
            String[] domainParts = domain.split("\\.");
            if (domainParts.length >= 2) {
                return domainParts[domainParts.length - 2];
            }
            return domain;
        }
        return "";
    }
}