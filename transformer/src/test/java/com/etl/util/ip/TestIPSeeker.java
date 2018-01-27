package com.etl.util.ip;

import com.jason.etl.util.ip.IPSeeker;

public class TestIPSeeker {
	public static void main(String[] args) {
		IPSeeker ipSeeker = IPSeeker.getInstance();
		System.out.println(ipSeeker.getCountry("120.197.87.216"));
		System.out.println(ipSeeker.getCountry("114.61.94.253"));
	}
}
