package com.practice.dataloader.mysql;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name = "p_post", indexes = {
		@Index(name = "idx_p_post_user_id", columnList = "user_id")
})
@Getter
@Setter
// MySQL p_post 테이블 생성 기준
public class Post {

	@Id
	private Long id;

	@Column(nullable = false)
	private Long userId;

	@Column(nullable = false, length = 200)
	private String title;

	@Column(nullable = false, length = 1000)
	private String content;

	@Column(nullable = false, length = 20)
	private String status;

	@Column(nullable = false)
	private LocalDateTime createdAt;

	@Column(nullable = false)
	private LocalDateTime updatedAt;

	protected Post() {
	}

}
