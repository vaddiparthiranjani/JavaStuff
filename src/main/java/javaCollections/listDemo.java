package javaCollections;


import java.util.ArrayList;
import java.util.List;
public class listDemo {
public static void main(String[] args)
    {
        List<String> list =new ArrayList<>();

        // list allows us to add duplicate values
        list.add("one");
        list.add("two");
        list.add("three");
        list.add("two");

        System.out.println(list);
        // List allows us to add Null

        list.add(null);
        list.add(null);
        System.out.println(list);

        // Access elements from the list

        System.out.println(list.get(0));
        System.out.println(list.get(3));
        System.out.println(list.get(4));
    }
}
