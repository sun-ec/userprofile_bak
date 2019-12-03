package cn.itcast.userprofile.platform.bean.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TagModelDto {
    private TagDto tag;
    private ModelDto model;
}