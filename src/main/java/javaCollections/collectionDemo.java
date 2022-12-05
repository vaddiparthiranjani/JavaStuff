package javaCollections;

import java.util.ArrayList;
import java.util.Collection;

public class collectionDemo {

    public static void main(String[] args)
    {

    Collection<String> fruit = new ArrayList<>();
    fruit.add("Apple");
    fruit.add("Banana");
    fruit.add("orange");
    fruit.add("Grapes");
    
    System.out.println(fruit);

    fruit.remove("Banana");
    System.out.println(fruit);

    System.out.println(fruit.contains("Apple"));

    fruit.forEach( ( element ) ->  { System.out.println(element); } );

    fruit.clear();
    System.out.println(fruit);
    }

}
