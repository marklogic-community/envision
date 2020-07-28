package com.marklogic.envision.system;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.envision.dataServices.SystemUtils;
import com.marklogic.grove.boot.AbstractController;
import com.marklogic.hub.impl.HubConfigImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

@RestController
@RequestMapping("/api/system")
public class SystemController extends AbstractController {

	@Autowired
	SystemController(HubConfigImpl hubConfig) {
		super(hubConfig);
	}

	@RequestMapping(value = "/reset", method = RequestMethod.POST)
	JsonNode reset(HttpSession session, HttpServletRequest request, HttpServletResponse response) {
		DatabaseClient client = getFinalClient();
		return SystemUtils.on(client).resetSystem();
	}
}
