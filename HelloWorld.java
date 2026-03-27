// HelloWorld.java

public class HelloWorld {
    public static void main(String[] args) {
        System.out.println("Hello, World!");

        // Variables and Data Types
        int myInt = 10;
        double myDouble = 20.5;
        char myChar = 'A';
        boolean myBoolean = true;

        System.out.println("myInt: " + myInt);
        System.out.println("myDouble: " + myDouble);
        System.out.println("myChar: " + myChar);
        System.out.println("myBoolean: " + myBoolean);

        // Conditional Statements
        if (myInt > 5) {
            System.out.println("myInt is greater than 5");
        } else {
            System.out.println("myInt is less than or equal to 5");
        }

        // Loops
        for (int i = 0; i < 5; i++) {
            System.out.println("Loop iteration: " + i);
        }

        // Methods
        greet("John");
    }

    public static void greet(String name) {
        System.out.println("Hello, " + name + "!");
    }
}