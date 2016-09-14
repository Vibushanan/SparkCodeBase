package com.slk.spark.SparkPractise;

import java.util.ArrayList;

/**
 * Hello world!
 *
 */
public class App 

{
	
	public static void printList(ArrayList<Person> al){
		
		
		System.out.println("Size--->"+al.size());
		for(Person p : al){
			
			System.out.println(p.getName());
			System.out.println(p.getAge());
		}
		
		System.out.println("---------------------------");
	}
	
	
    public static void main( String[] args )
    {
      ArrayList<Person> orignal = new ArrayList<Person>();
      
      
     Person p1 = new  Person("Shyamala",29);
     Person p2 = new  Person("Shyamala",28);
     Person p3 = new  Person("Saahil",1);
      
     orignal.add(p1);
     orignal.add(p2);
     orignal.add(p3);
      
     ArrayList<Person> Cloned = (ArrayList<Person>) orignal.clone();
      
      
      
     ArrayList<Person> Deletedd = new ArrayList<Person>();
    	
     Person pdel = Cloned.get(0);
     
     //pdel.setName("AAAA");
     Deletedd.add(pdel);
      
      
    printList(orignal);
     printList(Cloned);
     printList(Deletedd);
     
    System.out.println("---------------After Deletion ------------------------------");
    
    
    for(Person p : Deletedd){
    	
    	Cloned.remove(p);
    }
     
     printList(orignal);
     printList(Cloned);
     printList(Deletedd);
      
    }
}
