package cn.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;


public class HttpUtils {

	private static Logger logger = LoggerFactory.getLogger(HttpUtils.class);

	private static final String charset = "utf-8";

	public static String post(String path, String method, String params) {

		HttpURLConnection conn = null;
		try {
			logger.info(String.format("request-->Path[%s], RequestMethod[%s],RequestBody[%s]", path, method,
					URLDecoder.decode(params, charset)));
			boolean isPost = true;
			if (method.equals("GET")) {
				isPost = false;
				if (params != null && !params.equals(""))
					path += "?" + params;

			}

			URL url = new URL(path);
			conn = (HttpURLConnection) url.openConnection();
			conn.setConnectTimeout(5000);
			conn.setReadTimeout(5000);
			conn.setRequestMethod(method);
			conn.setUseCaches(false);
			conn.setDoOutput(true);
			conn.setRequestProperty("Connection", "Keep-Alive");
			conn.setRequestProperty("Charset", charset);

			if (isPost) {
				byte[] data = params.getBytes(charset);
				conn.setRequestProperty("Content-Length", String.valueOf(data.length));
				conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
				DataOutputStream outStream = new DataOutputStream(conn.getOutputStream());
				outStream.write(data);
				outStream.flush();
			}

			InputStream in = conn.getInputStream();
			StringBuffer sb = new StringBuffer();
			InputStreamReader reader = new InputStreamReader(in, charset);

			char[] buff = new char[1024];
			int len;
			while ((len = reader.read(buff)) > 0) {
				sb.append(buff, 0, len);
			}

			logger.info(String.format("response-->Path[%s], requestParma[%s],ResponseCode[%s], ResponseBody[%s]", path,
					URLDecoder.decode(params, charset), conn.getResponseCode(), sb.toString().replace("\n", "")));

			if (conn.getResponseCode() == 200) {
				if (!sb.toString().equals("")) {
					return sb.toString();
				} else {
					logger.error("Response empty string.");
				}
			} else {
				logger.warn("Response not 200 - " + conn.getResponseCode() + ", " + conn.getResponseMessage());
			}
		} catch (IOException ioe) {
			logger.error(String.format("Path[%s], GET RESPONSE CODE ERROR[%s]", path, ioe.getMessage()), ioe);
		} catch (Exception e) {
			logger.error(String.format("Path[%s], Error[%s]", path, e.getMessage()));
		}
		return null;
	}

	public static String doPost(String path, Map<String, Object> params) {
		return post(path, "POST", parse(params));
	}

	public static String doGet(String path, Map<String, Object> params) {
		return post(path, "GET", parse(params));
	}

	private static String parse(Map<String, Object> params) {
		if (params == null || params.equals(""))
			return "";

		StringBuilder builder = new StringBuilder();
		for (String key : params.keySet()) {
			try {
				builder.append(key + "=" + URLEncoder.encode(String.valueOf(params.get(key)), charset) + "&");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		return builder.toString();
	}

	public static void main(String[] args) {

		String url = "http://www.baidu.com";
		Map<String, Object> map = new HashMap<String, Object>();
		System.out.println(map);

	}
}
