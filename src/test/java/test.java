public class test {
    public static void main(String args[]) {
        String input="15:46:27";
        String output="15:49:19";

        long a=Long.parseLong(input.split(":")[0])*3600+Long.parseLong(input.split(":")[1])*60+Long.parseLong(input.split(":")[2]);

        long b=Long.parseLong(output.split(":")[0])*3600+Long.parseLong(output.split(":")[1])*60+Long.parseLong(output.split(":")[2]);

        System.out.println(b-a);
    }
}
