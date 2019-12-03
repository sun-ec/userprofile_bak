/**
 * Author itcast
 * Date 2019/11/23 16:59
 * Desc 演示Java8中接口的新特性
 */
public class TestDemo {
    public static void main(String[] args) {
        new IPhone().video();
        new XiaoMi().video();
    }
}

interface Phone {
    void call();
    //void video();
    // 在java8之前是不敢对接口进行扩展升级的,
    // 因为java8之前的接口中只能加抽象方法,那么所有的实现类都得修改代码去实现增加的抽象方法
    //那么如何解决?
    //在Java8之后,对接口增加了一些新特性
    default void video(){
        System.out.println("视频通话");
    }
    //那么这样在Java8以后的版本中就可以放心大胆的给接口进行功能的扩展,只需要将该功能作为接口的默认方法即可
    //实现类不需要做任何的修改,都具有了该功能!
}

class IPhone implements Phone{
    @Override
    public void call() {

    }
}
class XiaoMi implements Phone{
    @Override
    public void call() {

    }
}