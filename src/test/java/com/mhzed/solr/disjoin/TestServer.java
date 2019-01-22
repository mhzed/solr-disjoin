package com.mhzed.solr.disjoin;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class TestServer{

  protected static final Logger LOGGER = LoggerFactory.getLogger(TestServer.class);
	public class Config {
		public String name; // for logging
		public List<String> cmd; // the command
		public File stdout; // where to redirect std to
		public File stderr;
		public int port = 0; // wait for port to listen
    public Map<String, String> env = null;
    public Config(String name, List<String> cmd) {
      this.name = name;
      this.cmd = cmd;
    }
    public Config(String name, List<String> cmd, File stdout, File stderr, int port, Map<String, String> env) {
      this.name = name;
      this.cmd = cmd;
      this.stdout = stdout;
      this.stderr = stderr;
      this.port = port;
      this.env = env;
    }
	}
	protected Process server = null;

	public abstract Config config() throws Exception;

  public static Process exec(Config config) throws Exception {
		ProcessBuilder pb = new ProcessBuilder(config.cmd);
		Path currentRelativePath = Paths.get("./target");
		pb.directory(currentRelativePath.toFile());
		Map<String, String> env = pb.environment();
		if (config.env != null)
			config.env.forEach(env::put);

		Path logDir = Paths.get("./target/logs");
		Files.createDirectories(logDir);

		if (config.stdout == null) {
			Path outLog = Paths.get("./target/logs/" + config.name + "-out.log");
			Files.createFile(outLog);
			pb.redirectOutput(outLog.toFile());
		} else {
			pb.redirectOutput(config.stdout);
		}
		if (config.stderr == null) {
			Path errLog = Paths.get("./target/logs/" + config.name + "-err.log");
			Files.createFile(errLog);
			pb.redirectError(errLog.toFile());
		} else {
			pb.redirectError(config.stderr);
		}
		return pb.start();
  }
  public static void waitForPort(int nTries, int msSleep, int port, Process p) throws Exception {
    while (isPortFree(port) && nTries > 0) {
      LOGGER.debug("Waiting for listen at port {}...", port);
      Thread.sleep(msSleep);
      if (p!=null) {
        if (!p.isAlive()) {
          throw new Exception(String.format("Process died before listen at port %d", port));
        }
      }
      nTries--;
    }
    if (nTries == 0) {
      throw new Exception(String.format("Listen at port %d failed", port));
    }
  }

	public void launch() throws Exception {
    Config c = config();
    this.server = exec(c);
	}

	public void shutdown() throws Exception {
    if (this.server == null) return;
		this.server.destroy();
		this.server.waitFor();
		this.server = null;
	}

	public static boolean isPortFree(int port) {
		Socket clientSocket = null;
		try {
			clientSocket = new Socket();
			clientSocket.setSoTimeout(1000);
			clientSocket.connect(new InetSocketAddress("localhost", port));
			return false; // connect ok
		} catch (IOException e) {
		} finally {
			if (clientSocket != null) {
				try {
					clientSocket.close();
				} catch (IOException e) {
				}
			}
		}
		return true; // connect failed
	}

}
