package com.practice.managementconsole;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ManagementConsoleApplicationTests {

	@Test
	@DisplayName("애플리케이션_클래스확인")
	void 애플리케이션_클래스확인() {
		// given
		Class<ManagementConsoleApplication> applicationClass = ManagementConsoleApplication.class;

		// when
		String simpleName = applicationClass.getSimpleName();

		// then
		assertThat(simpleName).isEqualTo("ManagementConsoleApplication");
	}

}
