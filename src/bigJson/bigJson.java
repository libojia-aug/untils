package bigJson;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by Berger on 2016/12/12.
 */
public class bigJson {
    private static void loadData() {
        Path path = Paths.get("./input/JDComment/comment.json");
        System.out.println(path);
        int count = 0;
        try {
            FileInputStream fin = new FileInputStream(path.toFile());
            InputStreamReader isr = new InputStreamReader(fin, "GBK");
            BufferedReader bufr = new BufferedReader(isr);

            String s = bufr.readLine();
            JsonReader reader = new JsonReader(new StringReader(s));
            reader.setLenient(true);
            System.out.println(s.length());
            try {
                reader.beginArray();
                while (reader.hasNext()) {

                    reader.beginObject();
                    while (reader.hasNext()) {

                        switch (reader.peek()) {
                            case NUMBER: {
                                System.out.println(reader.nextInt());
                                if (reader.peek().equals(JsonToken.NAME)) {
                                    System.out.println(reader.nextName());
                                } else {
                                    reader.endObject();
                                }

                                reader.beginObject();
                                while (reader.hasNext()) {
                                    System.out.println(reader.nextName());
                                    switch (reader.peek()) {
                                        case STRING: {
                                            System.out.println(reader.nextString());
                                            break;
                                        }
                                        case BEGIN_ARRAY: {
                                            reader.beginArray();
                                            while (reader.hasNext()) {
                                                reader.beginObject();
                                                while (reader.hasNext()) {
                                                    System.out.println(reader.nextName());
                                                    if (reader.peek().equals(JsonToken.BEGIN_ARRAY)) {
                                                        reader.beginArray();
                                                        while (reader.hasNext()) {
                                                            reader.beginObject();
                                                            while (reader.hasNext()) {
                                                                System.out.println(reader.nextName());
                                                                if (reader.peek().equals(JsonToken.BEGIN_OBJECT)) {
                                                                    reader.beginObject();
                                                                    while (reader.hasNext()) {
                                                                        System.out.println(reader.nextName());
                                                                        System.out.println(reader.nextString());
                                                                    }
                                                                    reader.endObject();
                                                                } else {
                                                                    System.out.println(reader.nextString());
                                                                }

                                                            }
                                                            reader.endObject();
                                                        }
                                                        reader.endArray();
                                                    } else {
                                                        System.out.println(reader.nextString());
                                                    }

                                                }
                                                reader.endObject();
                                            }
                                            reader.endArray();
                                            break;
                                        }
                                        case NUMBER: {
                                            System.out.println(reader.nextDouble());
                                            break;
                                        }
                                        case BEGIN_OBJECT:{
                                            reader.beginObject();
                                            while (reader.hasNext()) {
                                                System.out.println(reader.nextName());
                                                System.out.println(reader.nextString());
                                            }
                                            reader.endObject();
                                            break;
                                        }
                                        default: {
                                            System.out.println(reader.nextString());
                                            System.out.println("err");

                                        }
                                    }

                                }
                                reader.endObject();
                                break;
                            }
                            case NAME: {
                                switch (reader.nextName()) {
                                    case "imageListCount":
                                        System.out.println(reader.nextString());
                                        break;
                                    case "maxPage":
                                        System.out.println(reader.nextString());
                                        break;
                                    case "comments":
                                        reader.beginArray();
                                        while (reader.hasNext()) {

                                            reader.beginObject();
                                            while (reader.hasNext()) {
                                                if (reader.nextName().equals("commentTags")) {
                                                    reader.beginArray();
                                                    while (reader.hasNext()) {
                                                        reader.beginObject();
                                                        while (reader.hasNext()) {
                                                            System.out.println(reader.nextName());
                                                            System.out.println(reader.nextString());
                                                        }
                                                        reader.endObject();
                                                    }
                                                    reader.endArray();
                                                } else {
                                                    System.out.println(reader.nextString());
                                                }

                                            }
                                            reader.endObject();
                                        }
                                        reader.endArray();
                                        break;
                                }
                                break;
                            }
                            case STRING: {
                                System.out.println(reader.nextString());
                                break;
                            }
                            case BEGIN_ARRAY: {
                                reader.beginArray();
                                while (reader.hasNext()) {
                                    reader.beginObject();
                                    while (reader.hasNext()) {
                                        System.out.println(reader.nextName());
                                        System.out.println(reader.nextString());
                                    }
                                    reader.endObject();
                                }
                                reader.endArray();
                                break;
                            }
                            case BEGIN_OBJECT:{
                                reader.beginObject();
                                while (reader.hasNext()) {
                                    System.out.println(reader.nextName());
                                    if(reader.peek().equals(JsonToken.BEGIN_ARRAY)){
                                        reader.beginArray();
                                        while (reader.hasNext()) {
                                            reader.beginObject();
                                            while (reader.hasNext()) {
                                                System.out.println(reader.nextName());
                                                if (reader.nextName().equals("commentTags")) {
                                                    reader.beginArray();
                                                    while (reader.hasNext()) {
                                                        reader.beginObject();
                                                        while (reader.hasNext()) {
                                                             System.out.println(reader.nextName());
                                                            System.out.println(reader.nextString());
                                                        }
                                                        reader.endObject();
                                                    }
                                                    reader.endArray();
                                                } else {
                                                    System.out.println(reader.nextString());
                                                }

                                            }
                                            reader.endObject();
                                        }
                                        reader.endArray();
                                    }else {
                                        System.out.println(reader.nextString());
                                    }

                                }
                                reader.endObject();
                                break;
                            }
                            default:
                                System.out.println(reader.nextString());
                                System.out.println("err");
                        }
                    }
                    reader.endObject();
                    System.exit(0);
                }

                reader.endArray();
//                System.out.println(count);
//                reader.beginObject();
//                while (reader.hasNext()) { // 如果有下一个数据就继续解析
//                    System.out.println(reader.peek());
//                    switch (reader.peek()){
//                        case NAME:
//                            System.out.println(reader.nextName());
//                            break;
//                        case NUMBER:
//                            System.out.println(reader.nextInt());
//                            break;
//                        case BEGIN_ARRAY:
//
//                        default:
//                            System.out.println("1");
//                    }
//
//                }
//                reader.endObject();

            } catch (IOException e) {
                // TODO 自动生成的 catch 块
                e.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return;
    }

    public static void main(String[] args) {
        loadData();
    }
}

