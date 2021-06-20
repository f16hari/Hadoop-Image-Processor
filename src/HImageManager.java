import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import java.awt.Color;
import javax.imageio.ImageIO;
import javax.swing.plaf.basic.BasicInternalFrameTitlePane.SystemMenuBar;

import java.awt.image.BufferedImage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HImageManager {

    static FileSystem Hdfs;

    static String HadoopWorkingDirectory;
    static int ImageHeight;
    static int ImageWidth;
    static String[] Classifications;

    static Scanner scanner;

    public static void Initialize() throws IOException {
        Properties prop = new Properties();
        FileInputStream configFileStream = new FileInputStream("HImageConfigurations.config");
        prop.load(configFileStream);

        Configuration conf = new Configuration();
        Hdfs = FileSystem.get(URI.create(prop.getProperty("HimageManager.HadoopURI")), conf);

        HadoopWorkingDirectory = prop.getProperty("HimageManager.HadoopDirectory");
        Classifications = prop.getProperty("HimageManager.Classifications").split(",");
        ImageHeight = Integer.parseInt(prop.getProperty("HimageManager.ImageHeight"));
        ImageWidth = Integer.parseInt(prop.getProperty("HimageManager.ImageWidth"));

        scanner = new Scanner(System.in);
    }

    public static void main(String[] args) throws IOException {
        Initialize();
        clearScreen();

        System.out.println("");
        System.out.println(
                "**********************************************Welcome to Hadoop Image Manager*******************************************");
        System.out.println("Enter your choice to proceed");
        System.out.println("1. Import Files into Hadoop dfs");
        System.out.println("2. Search in Hadoop dfs");
        int choice = scanner.nextInt();
        clearScreen();

        switch (choice) {
            case 1:
                importFilesIntoHadoop();
                break;
            case 2:
                System.out.println("Please enter path for your query image");
                String queryImagePath = scanner.next();
                Search(queryImagePath);
                break;
        }

        scanner.close();
    }

    private static void Search(String queryImagePath) throws IOException {
        clearScreen();
        System.out.println("Searching....");
        BufferedImage searchImage = ImageIO.read(new File(queryImagePath));
        String imageVector = getImageVector(searchImage, ImageHeight, ImageWidth);

        HashMap<String, String> bannerMap = new HashMap<>();

        for (String classification : Classifications) {
            BufferedReader br = new BufferedReader(new java.io.InputStreamReader(
                    Hdfs.open(new Path(HadoopWorkingDirectory + classification + "/banner.txt"))));
            bannerMap.put(classification, br.readLine());
            br.close();
        }

        String classification = getClassificationMatch(bannerMap, getIntVector(imageVector));

        printMatchingImageNames(classification, getIntVector(imageVector));
    }

    private static void printMatchingImageNames(String classification, int[] imageVector) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new java.io.InputStreamReader(
                Hdfs.open(new Path(HadoopWorkingDirectory + classification + "/" + classification + ".txt"))));

        String st;
        System.out.println();

        while ((st = bufferedReader.readLine()) != null) {
            String name = st.split(" ")[0];
            int[] vector = getIntVector(st.split(" ")[1]);

            if (Arrays.equals(vector, imageVector)) {
                System.out.println("Match found : " + classification + "/" + name);
            }
        }

        System.out.println("Please run hdfs dfs -copyToLocal /user/hariharan/Input/<path returned> found.jpg");
        System.out.println("To copy the file to your local file system");

        bufferedReader.close();
    }

    private static void importFilesIntoHadoop() throws IllegalArgumentException, IOException {
        System.out.println("Enter the path to Import Images from");
        scanner.nextLine();
        String sourceDirectory = scanner.nextLine();
        clearScreen();
        HashMap<String, String> bannerMap = initializeHadoopDirectory(sourceDirectory);
        clearScreen();
        addFilesToHdfs(sourceDirectory, bannerMap);
    }

    private static HashMap<String, String> initializeHadoopDirectory(String sourceDirectory)
            throws IllegalArgumentException, IOException {

        System.out.println("Intializing Hadoop Directory '" + HadoopWorkingDirectory + "'.....");
        System.out.println();

        HashMap<String, String> bannerMap = new HashMap<>();

        for (String classification : Classifications) {

            if (Hdfs.exists(new Path(HadoopWorkingDirectory + classification))) {
                Hdfs.delete(new Path(HadoopWorkingDirectory + classification), true);
            }

            BufferedImage bannerImage = new BufferedImage(ImageHeight, ImageWidth, BufferedImage.TYPE_INT_ARGB);
            bannerImage = ImageIO.read(new File(sourceDirectory + classification + "/banner.jpg"));

            Hdfs.mkdirs(new Path(HadoopWorkingDirectory + classification));
            FSDataOutputStream fStream = Hdfs.create(new Path(HadoopWorkingDirectory + classification + "/banner.txt"));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fStream, StandardCharsets.UTF_8));

            String vector = getImageVector(bannerImage, ImageHeight, ImageWidth);
            bannerMap.put(classification, vector);
            bw.write(vector);
            bw.close();
            fStream.close();

            System.out.println("Hadoop directory initialized for classification : " + classification);
        }

        System.out.print("Press any key to continue...");
        scanner.nextLine();
        return bannerMap;
    }

    private static void addFilesToHdfs(String sourceDirectory, HashMap<String, String> bannerMap)
            throws IllegalArgumentException, IOException {

        System.out.println("Import Files......");
        System.out.println();

        File[] files = new File(sourceDirectory).listFiles();
        HashMap<String, HashMap<String, String>> allImageVectors = new HashMap<>();

        for (String bannerEntry : bannerMap.keySet()) {
            allImageVectors.put(bannerEntry, new HashMap<>());
        }

        for (File file : files) {
            if ((!file.isDirectory()) && (file.getAbsolutePath().endsWith(".jpg"))) {
                BufferedImage image = ImageIO.read(file);
                String vector = getImageVector(image, ImageHeight, ImageWidth);
                int[] imageVector = getIntVector(vector);

                String classification = "";

                classification = getClassificationMatch(bannerMap, imageVector);

                allImageVectors.get(classification).put(file.getName(), vector);

                Hdfs.copyFromLocalFile(new Path(file.getPath()),
                        new Path(HadoopWorkingDirectory + classification + "/" + file.getName()));

                System.out.println(
                        "Image :" + file.getName() + " imported to : " + HadoopWorkingDirectory + classification);
            }
        }

        addAllVectors(allImageVectors);

        System.out.println("All Files Imported Successfully, Press any key to continue....");
        scanner.nextLine();
    }

    private static void addAllVectors(HashMap<String, HashMap<String, String>> allImageVectors) throws IOException {

        for (String classification : allImageVectors.keySet()) {

            FSDataOutputStream fStream = Hdfs
                    .create(new Path(HadoopWorkingDirectory + classification + "/" + classification + ".txt"));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fStream, StandardCharsets.UTF_8));

            HashMap<String, String> allVectors = allImageVectors.get(classification);

            for (Map.Entry<String, String> vector : allVectors.entrySet()) {
                bw.write(vector.getKey() + " " + vector.getValue());
                bw.newLine();
            }

            bw.close();
            fStream.close();
        }
    }

    private static String getImageVector(BufferedImage image, int height, int width) {
        long red = 0;
        long blue = 0;
        long green = 0;
        int totalPixals = height * width;

        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                Color color = new Color(image.getRGB(i, j));
                red += color.getRed();
                blue += color.getBlue();
                green += color.getGreen();
            }
        }

        return red / totalPixals + "," + green / totalPixals + "," + blue / totalPixals;
    }

    private static int[] getIntVector(String vector) {
        int[] bannerVector = new int[3];
        int i = 0;

        for (String intensity : vector.split(",")) {
            bannerVector[i++] = Integer.parseInt(intensity);
        }
        return bannerVector;
    }

    private static String getClassificationMatch(HashMap<String, String> bannerMap, int[] imageVector) {
        double min = Double.MAX_VALUE;
        String match = "";

        for (Map.Entry<String, String> bannerEntry : bannerMap.entrySet()) {
            int[] classificationVector = getIntVector(bannerEntry.getValue());
            double distance = getEuclideanDistance(classificationVector, imageVector);
            if (min > distance) {
                min = distance;
                match = bannerEntry.getKey();
            }
        }
        return match;
    }

    private static double getEuclideanDistance(int[] vector1, int[] vector2) {
        int sum = 0;

        for (int i = 0; i < 3; i++) {
            sum += Math.pow((double) (vector1[i] - vector2[i]), 2);
        }

        return Math.sqrt(sum);
    }

    private static void clearScreen() {
        System.out.print("\033[H\033[2J");
        System.out.flush();
    }
}