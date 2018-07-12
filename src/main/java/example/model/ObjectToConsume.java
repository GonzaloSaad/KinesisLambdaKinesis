package example.model;

public class ObjectToConsume {
    private String name;
    private int age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "ObjectToConsume{" +
                "name='" + name + "'" +
                ", age=" + age +
                '}';
    }

    public ObjectToProduce toProduce(){
        ObjectToProduce object = new ObjectToProduce();
        object.setAge(age + 10);
        object.setName(name + "L");
        return object;
    }
}
