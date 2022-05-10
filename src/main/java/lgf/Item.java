package lgf;

/**
 * @author zhuang.ma
 * @date 2022/5/6
 */
public class Item {
    private String name;
    private String id;

    public Item(String name, String id) {
        this.name = name;
        this.id = id;
    }

    public Item() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Item{" +
                "name='" + name + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
