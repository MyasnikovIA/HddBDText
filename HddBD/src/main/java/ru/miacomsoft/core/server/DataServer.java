package ru.miacomsoft.core.server;

import ru.miacomsoft.core.BinaryDataManager;
import ru.miacomsoft.core.MemoryManager;
import ru.miacomsoft.core.server.protocol.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class DataServer {
    private final int port;
    private final BinaryDataManager dataManager;
    private final MemoryManager memoryManager;
    private final ExecutorService clientExecutor;
    private final AtomicBoolean running;
    private ServerSocket serverSocket;

    public DataServer(int port, String dataPath, String indexPath, long maxMemoryBytes) throws IOException {
        this.port = port;
        this.memoryManager = new MemoryManager(maxMemoryBytes);
        this.dataManager = new BinaryDataManager(dataPath, indexPath, memoryManager);
        this.clientExecutor = Executors.newCachedThreadPool();
        this.running = new AtomicBoolean(false);
    }

    public DataServer(int port, String dataPath, String indexPath) throws IOException {
        this(port, dataPath, indexPath, 100 * 1024 * 1024); // 100 MB default
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running.set(true);

        System.out.println("DataServer started on port " + port);
        System.out.println("Memory limit: " + (memoryManager.getMaxMemory() / (1024 * 1024)) + " MB");
        System.out.println("Data file: " + dataManager.getDataFileName());
        System.out.println("Index file: " + dataManager.getIndexFileName());

        while (running.get()) {
            try {
                Socket clientSocket = serverSocket.accept();
                clientExecutor.execute(new ClientHandler(clientSocket, dataManager, memoryManager));
            } catch (IOException e) {
                if (running.get()) {
                    System.err.println("Error accepting client connection: " + e.getMessage());
                }
            }
        }
    }

    public void stop() {
        running.set(false);
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing server socket: " + e.getMessage());
        }

        clientExecutor.shutdown();
        try {
            dataManager.close();
            memoryManager.close();
        } catch (Exception e) {
            System.err.println("Error closing resources: " + e.getMessage());
        }

        System.out.println("DataServer stopped");
    }

    public BinaryDataManager getDataManager() {
        return dataManager;
    }

    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    public boolean isRunning() {
        return running.get();
    }
}