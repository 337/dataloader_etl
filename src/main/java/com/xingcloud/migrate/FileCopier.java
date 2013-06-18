package com.xingcloud.migrate;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileCopier implements Runnable {

    private File src;

    private File dist;

    private boolean list;

    public FileCopier(File src, File dist, boolean list) {
        this.src = src;
        this.dist = dist;
        this.list = list;
    }

    public void check() throws FileNotFoundException {
        if (src == null || dist == null) {
            throw new FileNotFoundException("Src or dist can not be null.");
        }
        if (!src.exists()) {
            throw new FileNotFoundException("Src file(" + src.getAbsolutePath()
                    + ") does not exist.");
        }

    }

    public void copy() throws FileNotFoundException {
        File distFather = dist.getParentFile();
        if (!distFather.exists()) {
            distFather.mkdirs();
        }

        FileInputStream fis = new FileInputStream(src);
        FileOutputStream fos = new FileOutputStream(dist);

        FileChannel fcin = fis.getChannel();
        FileChannel fcout = fos.getChannel();
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        try {
            while (true) {
                buffer.clear();
                int r = fcin.read(buffer);
                if (r == -1) {
                    break;
                }
                buffer.flip();
                fcout.write(buffer);
            }
            File copied = new File(dist.getAbsolutePath() + ".copied");
            try {
                copied.createNewFile();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        } catch (IOException e) {
            File failure = new File(dist.getAbsolutePath() + ".failure");
            try {
                failure.createNewFile();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        } finally {
            try {
                if (fcin != null) {
                    fcin.close();
                }
                if (fcout != null) {
                    fcout.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void run() {
        try {
            check();
        } catch (FileNotFoundException e1) {
            System.out.println(e1.getMessage());
        }
        if (list) {
            System.out.println("[FROM] " + src.getAbsolutePath() + " [TO] "
                    + dist.getAbsolutePath());
        } else {
            System.out.println("[COPING] [" + Thread.currentThread().getName()
                    + "] " + src.getAbsolutePath());
            try {
                copy();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        File f = new File("e:/1/2/3/4/5.log");
        if (!f.getParentFile().exists()) {
            f.getParentFile().mkdirs();
        }
    }

}
