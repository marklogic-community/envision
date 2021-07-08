package com.marklogic.envision.flows;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.FailedRequestException;
import com.marklogic.envision.config.EnvisionConfig;
import com.marklogic.envision.deploy.DeployService;
import com.marklogic.envision.hub.HubClient;
import com.marklogic.envision.pojo.StatusMessage;
import com.marklogic.hub.FlowManager;
import com.marklogic.hub.StepDefinitionManager;
import com.marklogic.hub.dataservices.FlowService;
import com.marklogic.hub.dataservices.StepService;
import com.marklogic.hub.flow.Flow;
import com.marklogic.hub.flow.FlowInputs;
import com.marklogic.hub.flow.impl.FlowRunnerImpl;
import com.marklogic.hub.impl.FlowManagerImpl;
import com.marklogic.hub.impl.ScaffoldingImpl;
import com.marklogic.hub.impl.StepDefinitionManagerImpl;
import com.marklogic.hub.scaffold.Scaffolding;
import com.marklogic.hub.step.StepDefinition;
import com.marklogic.hub.step.impl.CustomStepDefinitionImpl;
import com.marklogic.hub.step.impl.Step;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class FlowsService {
	final private EnvisionConfig envisionConfig;
	final private ObjectMapper mapper = new ObjectMapper();
	final private FlowManager flowManager;
	final private StepDefinitionManager stepDefinitionManager;
	final private DeployService deployService;
	final private SimpMessagingTemplate template;
	final private Scaffolding scaffolding;

	@Autowired
	FlowsService(
		EnvisionConfig envisionConfig,
		DeployService deployService,
		SimpMessagingTemplate template
	) {
		this.envisionConfig = envisionConfig;
		this.flowManager = new FlowManagerImpl(envisionConfig.getHubConfig());
		this.stepDefinitionManager = new StepDefinitionManagerImpl(envisionConfig.getHubConfig());
		this.deployService = deployService;
		this.template = template;
		this.scaffolding = new ScaffoldingImpl(envisionConfig.getHubConfig());
	}

	public JsonNode getJsonFlow(HubClient client, String flowName) {
		return mapper.valueToTree(getFlow(client, flowName));
	}

	public Flow getFlow(HubClient client, String flowName) {
		JsonNode flow = null;
		try {
			flow = newFlowService(client.getStagingClient()).getFullFlow(flowName);
		} catch (FailedRequestException ex) {
			// We want to create a new flow if it doesn't exist
		}
		if (flow != null) {
			return flowManager.createFlowFromJSON(flow);
		}

		// create the flow if it doesn't exist
		Flow newFlow = flowManager.createFlow(flowName);
		flowManager.saveFlow(newFlow);
		deployService.loadFlow(client, newFlow);
		return newFlow;
	}

	public Flow createFlowFromJSON(JsonNode flowJson) {
		return flowManager.createFlowFromJSON(flowJson);
	}

	public void createFlow(HubClient hubClient, JsonNode flowJson) {
		Flow flow = createFlowFromJSON(flowJson);
		flowManager.saveFlow(flow);
		deployService.loadFlow(hubClient, flow);
	}

	public void deleteFlow(HubClient hubClient, String flowName) {
		Flow flow = flowManager.getFlow(flowName);
		flow.getSteps().forEach((stepNumber, step) -> {
			StepService.on(hubClient.getStagingClient()).deleteStep(step.getStepDefinitionType().toString(), step.getName());
		});
		flowManager.deleteFlow(flowName);
	}

	public void deleteAllFlows(HubClient hubClient) {
		List<String> flowNames = flowManager.getFlows().stream().map(Flow::getName).collect(Collectors.toList());
		flowNames.forEach(flowName -> {
			Flow flow = flowManager.getFlow(flowName);
			flow.getSteps().forEach((stepName, step) -> flowManager.deleteStep(flow, stepName));
			deleteFlow(hubClient, flowName);
		});
	}

	public StepDefinition getCustomStep(String stepName) {
		return stepDefinitionManager.getStepDefinition(stepName, StepDefinition.StepDefinitionType.CUSTOM);
	}

	public JsonNode getFlows(DatabaseClient client) {
		return newFlowService(client).getFlowsWithStepDetails();
	}

	public void createStep(HubClient hubClient, String flowName, JsonNode... stepsJson) {
		createStep(hubClient, getFlow(hubClient, flowName), stepsJson);
	}

	public void createStep(HubClient hubClient, Flow flow, JsonNode... stepsJson) {
		List<IOException> ioExceptions = new ArrayList<>();
		for (JsonNode stepJson: stepsJson) {
			String stepType = stepJson.get("stepDefinitionType").asText();
			String stepName = stepJson.get("name").asText();
			String stepId = stepJson.path("stepId").asText(stepName + "-" + stepType.toLowerCase());
			String sourceCollection = stepJson.get("sourceCollection").asText();
			String sourceQuery = stepJson.get("sourceQuery").asText();
			if (hubClient.isMultiTenant()) {
				// update the permissions to be the user's unique role
				// so that when the flow runs, the output docs are visible only to the current user
				// user's unique role is an md5 hash of the username
				String email = hubClient.getUsername();
				String roleName = DigestUtils.md5Hex(email);
				((ObjectNode) stepJson).put("permissions", String.format("%s,read,%s,update", roleName, roleName));
				ArrayNode collections = (ArrayNode) stepJson.get("collections");
				if (collections == null) {
					collections = ((ObjectNode) stepJson).putArray("collections");
				}
				collections.add("http://marklogic.com/envision/user/" + hubClient.getUsername());
			}

			StepService.on(hubClient.getStagingClient()).saveStep(stepType, stepJson, false, false);
			try {
				saveStepToProject(stepType, stepJson);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

			Map<String, Step> steps = flow.getSteps();
			final String[] existingIndex = {String.valueOf(steps.size() + 1)};

			// see if the step already exists. happens when updating
			steps.forEach((idx, step1) -> {
				if (stepId.equals(step1.getStepId())) {
					existingIndex[0] = idx;
				}
			});
			Step step = new Step();
			step.setStepId(stepId);
			step.setName(stepName);
			step.setStepDefinitionType(StepDefinition.StepDefinitionType.getStepDefinitionType(stepType));

			steps.put(existingIndex[0], step);

			if (stepType.toUpperCase().equals("CUSTOM")) {
				try {
					ObjectMapper om = new ObjectMapper();
					JsonNode node = om.readTree(String.format("{\"name\":\"%s\",\"type\":\"custom\" }",stepName));
					CustomStepDefinitionImpl stepDefinition = (CustomStepDefinitionImpl) stepDefinitionManager.createStepDefinitionFromJSON(node);

					Map stepOptions = stepDefinition.getOptions();
					stepOptions.put("sourceCollection", sourceCollection);
					stepOptions.put("sourceQuery", sourceQuery);
					stepDefinition.setOptions(stepOptions);

					stepDefinition.setModulePath(String.format("/custom-modules/custom/%s/main.sjs", stepName));
					stepDefinitionManager.saveStepDefinition(stepDefinition);
					deployService.loadStepDefinition(hubClient, stepDefinition);
					scaffolding.createCustomModule(stepName,stepType);
					deployService.loadHubModules(hubClient);

				} catch (IOException e) {
					// Save IOException to be thrown later
					ioExceptions.add(e);
				}
			}
		}
		flowManager.saveFlow(flow);
		deployService.loadFlow(hubClient, flow);
		if (!ioExceptions.isEmpty()) {
			throw new RuntimeException("Failed to save mapping to project", ioExceptions.get(0));
		}
	}

	public void deleteStep(HubClient hubClient, String flowName, String stepName) {
		Flow flow =  getFlow(hubClient, flowName);
		String stepKey = getStepKeyByName(flow, stepName);
		if (stepKey != null) {
			Step step = flow.getStep(stepKey);
			StepDefinition.StepDefinitionType stepType = step.getStepDefinitionType();
			if (step.getStepId() != null) {
				StepService.on(hubClient.getStagingClient()).deleteStep(stepType.toString(), stepName);
			}
			if (stepType.equals(StepDefinition.StepDefinitionType.CUSTOM)){
				CustomStepDefinitionImpl sdm = (CustomStepDefinitionImpl)stepDefinitionManager.getStepDefinition(stepName,stepType);

				stepDefinitionManager.deleteStepDefinition(sdm);
				deployService.deleteStepDefinition(hubClient,stepName);
				deployService.deleteCustomModule(hubClient, sdm.getModulePath());

				Path customModuleDir = hubClient.getHubConfig().getHubProject().getCustomModuleDir(stepName, stepType.toString().toLowerCase());
				File moduleFile = customModuleDir.resolve("main.sjs").toFile();
				if (moduleFile.exists()) {
					moduleFile.delete();
				}
			}
			Map<String, Step> steps = flow.getSteps();
			steps.remove(stepKey);

			Map<String, Step> newStepMap = new LinkedHashMap<>();
			Step[] stepValues = steps.values().toArray(new Step[0]);
			for (int i = 0; i < stepValues.length; i++) {
				newStepMap.put(String.valueOf(i + 1), stepValues[i]);
			}
			flow.setSteps(newStepMap);
			// save to disk
			flowManager.saveFlow(flow);
			// save to MarkLogic
			deployService.loadFlow(hubClient, flow);
		}
	}

	@Async
	public void runStepsAsync(HubClient hubClient, String flowName, JsonNode steps) {
		runSteps(hubClient, flowName, steps);
	}

	public void runSteps(HubClient hubClient, String flowName, JsonNode steps) {
		try {
			ObjectReader reader = mapper.readerFor(new TypeReference<String[]>() {});
			String[] stepsList = reader.readValue(steps);
			FlowInputs inputs = new FlowInputs(flowName, stepsList);
			FlowRunnerImpl flowRunner = new FlowRunnerImpl(hubClient.getHubConfig(), flowManager);
			flowRunner.onStatusChanged((jobId, step, jobStatus, percentComplete, successfulEvents, failedEvents, message) -> {
				StatusMessage msg = StatusMessage.newStatus(jobId)
					.withMessage(message)
					.withPercentComplete(percentComplete);
				this.template.convertAndSend("/topic/status", msg);
			});
			flowRunner.runFlow(inputs);
			flowRunner.awaitCompletion();
		}
		catch(Exception e) {
			throw new RuntimeException("invalid steps", e);
		}
	}

	private String getStepKeyByName(Flow flow, String stepName) {
		Map<String, Step> steps = flow.getSteps();
		for (Map.Entry<String, Step> entry : steps.entrySet()) {
			if (entry.getValue().getName().equals(stepName)) {
				return entry.getKey();
			}
		}
		return null;
	}

	private String getEntityTypeFromStep(JsonNode step) {
		JsonNode optionsRoot = step.has("options") ? step.get("options") : step;
		return optionsRoot.path("targetEntityType").asText(optionsRoot.path("targetEntity").asText());
	}

	public void saveStepToProject(String stepType, JsonNode step) throws IOException {
		File stepPathDir = envisionConfig.dhfDir.toPath().toAbsolutePath().resolve("steps").resolve(stepType.toLowerCase()).toAbsolutePath().toFile();
		if (!stepPathDir.exists()) {
			stepPathDir.mkdirs();
		}
		File stepFile = stepPathDir.toPath().resolve(step.get("name").asText() + ".step.json").toFile();
		if (stepFile.exists()) {
			// emulate the MarkLogic save and don't drop unspecified properties
			JsonNode existingStep = mapper.readTree(new FileReader(stepFile));
			Iterator<String> fieldNames = existingStep.fieldNames();
			while (fieldNames.hasNext()) {
				String fieldName = fieldNames.next();
				if (!step.has(fieldName)) {
					((ObjectNode)step).set(fieldName, existingStep.get(fieldName));
				}
			}
		} else {
			stepFile.createNewFile();
		}
		try(FileWriter fw = new FileWriter(stepFile)) {
			fw.write(step.toPrettyString());
		}
	}

	private FlowService newFlowService(DatabaseClient client) {
		return FlowService.on(client);
	}
}
