package cn.itcast.userprofile.platform.service;

import cn.itcast.userprofile.platform.bean.dto.ModelDto;
import cn.itcast.userprofile.platform.bean.dto.TagDto;
import cn.itcast.userprofile.platform.bean.dto.TagModelDto;

import java.util.List;

public interface TagService {
    public void saveTags(List<TagDto> tags);

    List<TagDto> findByPid(Long pid);

    List<TagDto> findByLevel(Integer level);

    void addTagModel(TagDto tag, ModelDto model);

    List<TagModelDto> findModelByPid(Long pid);

    void addDataTag(TagDto tagDto);

    void updateModelState(Long id, Integer state);
}
