package com.ae.sdk;

public class Test {
	public static void main(String[] args) {
		AnalyticsEngineSDK.onChargeSuccess("orderid123", "jasonzhnang123");
		AnalyticsEngineSDK.onChargeRefund("orderid456", "jasonzhang456");
	}
}
