public class Main {

    public static void main(String[] args) throws Exception {
        Abox abox = new Abox();
        System.out.println("starting");

<<<<<<< HEAD
        //abox.transformDemographics();
        abox.transformFacebookData();
=======
        Abox.transformDemographics();
>>>>>>> b0c34164d4de78a0f75e278808e8e0cbc0b01743


        System.out.println("ending");
    }
}