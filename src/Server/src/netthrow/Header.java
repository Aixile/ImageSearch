package netthrow;


import java.net.*;
import java.util.*;
import java.util.regex.*;

public class Header {
    public String method;
    public FileInfo file;

    public static Header DecodeHeader(String string) throws ProtocolException {
        Header header = new Header();
        Scanner scanner = new Scanner(string.substring(0, string.length() - 2));

        header.method = scanner.nextLine();

        while (scanner.hasNextLine()) {
            String[] line = scanner.nextLine().split(":", 2);
            if (line.length != 2) throw new ProtocolException();

            String key = line[0].trim();
            String value = line[1].trim();

            switch (key) {
                case "File":
                    header.file = decodeFile(value);
                    break;
            }
        }
        scanner.close();

        return header;
    }

    private static FileInfo decodeFile(String string) throws ProtocolException {
        FileInfo fileInfo = new FileInfo();
        Pattern pattern = Pattern.compile("^filePath=\"(.*?)\"; length=(\\d+)$");
        Matcher matcher = pattern.matcher(string);
        if (!matcher.matches()) throw new ProtocolException();

        fileInfo.filePath = matcher.group(1);
        fileInfo.length = Long.parseLong(matcher.group(2));

        return fileInfo;
    }
}