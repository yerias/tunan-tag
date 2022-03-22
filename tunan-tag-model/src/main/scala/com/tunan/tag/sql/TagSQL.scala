package com.tunan.tag.sql

/**
 * @Auther: 李沅芮
 * @Date: 2022/3/22 08:30
 * @Description:
 */
object TagSQL {

    // Basic Tag table config
    def tagTable(tagId: Long): String = {
        s"""
		   |(
		   |SELECT `id`,
		   | `name`,
		   | `rule`,
		   | `level`
		   |FROM `sys_tag`.`tag_basic_tag`
		   |WHERE id = $tagId
		   |UNION
		   |SELECT `id`,
		   | `name`,
		   | `rule`,
		   | `level`
		   |FROM `sys_tag`.`tag_basic_tag`
		   |WHERE pid = $tagId
		   |ORDER BY `level` ASC, `id` ASC
		   |) AS basic_tag
		   |""".stripMargin
    }
}
