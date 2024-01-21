package io.bhex.broker.quote.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

public class NetUtil {
    public static String getIpAddress() {
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip = null;

            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
                if (netInterface.isLoopback() || netInterface.isVirtual() || !netInterface.isUp()) {
                    continue;
                } else {
                    Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                    while (addresses.hasMoreElements()) {
                        ip = addresses.nextElement();
                        if (ip != null && ip instanceof Inet4Address) {
                            return ip.getHostAddress();
                        }
                    }
                }
            }
        } catch (Exception e) {
            return "127.0.0.1";
        }
        return "127.0.0.1";
    }

    public static Long getIp() {
        String ipaddr = getIpAddress();
        String ip[] = ipaddr.split("\\.");
        Long ipLong = 256 * 256 * 256 * Long.parseLong(ip[0]) +
            256 * 256 * Long.parseLong(ip[1]) +
            256 * Long.parseLong(ip[2]) +
            Long.parseLong(ip[3]);
        return ipLong;
    }

    public static String getIp(long ip) {
        return ((ip >> 24) & 0xFF) + "."
            + ((ip >> 16) & 0xFF) + "."
            + ((ip >> 8) & 0xFF) + "."
            + (ip & 0xFF);
    }

    public static void main(String[] args) {
        System.out.println(getIpAddress());
    }
}

