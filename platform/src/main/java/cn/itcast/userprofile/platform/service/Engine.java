package cn.itcast.userprofile.platform.service;


import cn.itcast.userprofile.platform.bean.dto.ModelDto;

public interface Engine {

    void startModel(ModelDto modelDto);
    void stopModel(ModelDto modelDto);
}
