describe('Integrate Tab', () => {

	beforeEach(() => {
		cy.server()
		cy.route('/api/auth/status', {"appName":null,"authenticated":true,"username":"admin","disallowUpdates":false,"appUsersOnly":false})
		cy.route({
			method: 'PUT',
			url: '/api/models/',
			status: 204,
			response: {}
		})
		cy.route('GET', '/api/models/', [])
		cy.route('/api/auth/profile', {"username":"admin","fullname":null,"emails":null})
		cy.route('/api/models/current', 'fixture:model.json')
		cy.route('GET', '/api/entities', [])
		cy.route('GET', '/api/flows/21232f297a57a5a743894a0e4a801fc3', 'fixture:flow-no-steps.json')
		cy.route('GET', '/api/flows', 'fixture:flowsEmpty.json')
		cy.route('GET', '/api/jobs?flowName=21232f297a57a5a743894a0e4a801fc3', 'fixture:jobs.json')
		cy.route('GET', '/api/flows/newStepInfo', 'fixture:newStepInfoEmptyData.json')
		cy.route('POST', '/api/mastering/notifications', 'fixture:notificationsPage1.json')
		cy.route('GET', '/api/flows/mappings/functions', 'fixture:functions.json')
		cy.route('GET', '/api/flows/mappings/21232f297a57a5a743894a0e4a801fc3-MappingTest', 'fixture:maptest-mapping.json')
		cy.route('GET', '/v1/resources/mlCollections*', 'fixture:sample-doc-uris.json')
		cy.route('POST', '/api/flows/mappings', {})
		cy.route('POST', /\/api\/flows\/mappings\/validate.*/, 'fixture:validate-sample-doc.json')
		cy.route('POST', '/api/flows/mappings/sampleDoc', 'fixture:mapping-sample-doc.json')
		cy.route('POST', '/api/flows/mappings/preview', 'fixture:mapping-preview.json')
		cy.route('POST', '/api/flows/steps', {}).as('updateStep')
		cy.route('POST', '/api/flows/steps/delete', {}).as('deleteStep')
		cy.route('PUT', '/api/flows/21232f297a57a5a743894a0e4a801fc3', {}).as('saveFlow')
		cy.route('POST', '/api/system/deleteCollection', {}).as('deleteCollection')
	})

	it('should suggest uploading data', () => {
		// cy.route('GET', '/api/flows', 'fixture:flowsEmpty.json')
		cy.visit('/integrate')
		cy.contains('Start by Uploading data.')
		cy.get('[data-cy="integrate.addStepBtn"]').should('be.disabled')
		cy.get('[data-cy="integrate.runStepBtn"]').should('be.disabled')

	})

	it('should suggest creating a data model', () => {
		cy.route('GET', '/api/flows/newStepInfo', 'fixture:newStepInfo.json')
		cy.visit('/integrate')
		cy.contains('Start by creating a data model')
		cy.get('[data-cy="integrate.addStepBtn"]').should('be.disabled')
		cy.get('[data-cy="integrate.runStepBtn"]').should('be.disabled')
	})

	it('should allow new steps', () => {
		cy.route('GET', '/api/flows/newStepInfo', 'fixture:newStepInfo.json')
		cy.route('GET', '/api/entities', 'fixture:entities.json')
		cy.visit('/integrate')
		cy.get('[data-cy="integrate.addStepBtn"]').should('not.be.disabled')
		cy.get('[data-cy="integrate.runStepBtn"]').should('be.disabled')
	})

	it('shows all the steps', () => {
		cy.route('GET', '/api/flows/newStepInfo', 'fixture:newStepInfo.json')
		cy.route('GET', '/api/entities', 'fixture:entities.json')
		cy.route('GET', '/api/flows/', 'fixture:flowsEnvision.json')
		cy.route('GET', '/api/flows/21232f297a57a5a743894a0e4a801fc3', 'fixture:flow-envision.json')
		cy.visit('/integrate')
		cy.get('[data-cy="integrate.addStepBtn"]').should('not.be.disabled')
		cy.get('[data-cy="integrate.runStepBtn"]').should('not.be.disabled')

		cy.get('.step-wrapper').should('have.length', 3)
	})

	it('properly shows manage data', () => {
		cy.route('GET', '/api/flows/newStepInfo', 'fixture:newStepInfo.json')
		cy.route('GET', '/api/entities', 'fixture:entities.json')
		cy.route('GET', '/api/flows/21232f297a57a5a743894a0e4a801fc3', 'fixture:flow-envision.json')
		cy.visit('/integrate')
		cy.get('[data-cy="manageData.table"] tr').should('have.length', 8)
		cy.get('[data-cy="manageData.table"] [data-cy="deleteDataConfirm.deleteButton"]').should('have.length', 8)
		cy.get('[data-cy="manageData.table"] [data-cy="deleteDataConfirm.deleteButton"]:disabled').should('have.length', 6)

		cy.get('[data-cy="manageData.table"] [data-cy="deleteDataConfirm.deleteButton"]').first().click()
		cy.get('button').contains('Cancel').click()
		cy.route('GET', '/api/flows/newStepInfo', 'fixture:newStepInfoPostDelete.json')
		cy.get('[data-cy="manageData.table"] [data-cy="deleteDataConfirm.deleteButton"]').first().click()
		cy.get('button').contains('Delete').click()
		cy.wait('@deleteCollection')
			.its('request.body')
				.should(body => {
					expect(body).to.deep.equal({"collections": ["Customer"], "database": "final"})
				})
		cy.get('[data-cy="manageData.table"] [data-cy="deleteDataConfirm.deleteButton"]:disabled').should('have.length', 7)

		cy.get('[data-cy="manageData.toggle"]').click()
		cy.get('[data-cy="manageData.table"] [data-cy="deleteDataConfirm.deleteButton"]:disabled').should('have.length', 0)

		cy.get('[data-cy="manageData.toggle"]').click()
		cy.get('[data-cy="manageData.table"] [data-cy="deleteDataConfirm.deleteButton"]:disabled').should('have.length', 7)
	})

	it('delete all data', () => {
		cy.route('GET', '/api/flows/newStepInfo', 'fixture:newStepInfo.json')
		cy.route('GET', '/api/entities', 'fixture:entities.json')
		cy.route('GET', '/api/flows/21232f297a57a5a743894a0e4a801fc3', 'fixture:flow-envision.json')
		cy.visit('/integrate')
		cy.get('[data-cy="manageData.table"] tr').should('have.length', 8)
		cy.get('[data-cy="manageData.table"] [data-cy="deleteDataConfirm.deleteButton"]').should('have.length', 8)
		cy.get('[data-cy="manageData.table"] [data-cy="deleteDataConfirm.deleteButton"]:disabled').should('have.length', 6)

		cy.get('[data-cy="manageData.deleteAll"] [data-cy="deleteDataConfirm.deleteButton"]').click()
		cy.get('button').contains('Cancel').click()
		cy.route('GET', '/api/flows/newStepInfo', 'fixture:newStepInfoPostDelete.json')
		cy.get('[data-cy="manageData.deleteAll"] [data-cy="deleteDataConfirm.deleteButton"]').click()
		cy.get('button').contains('Delete').click()
		cy.wait('@deleteCollection')
			.its('request.body')
				.should(body => {
					expect(body).to.deep.equal({"collections": ["Customer", "Department", "Employee", "JobOpening", "JobReview", "Order", "Product", "Resume"], "database": "final"})
				})

		cy.get('[data-cy="manageData.table"] [data-cy="deleteDataConfirm.deleteButton"]:disabled').should('have.length', 8)

		cy.get('[data-cy="manageData.toggle"]').click()
		cy.get('[data-cy="manageData.table"] [data-cy="deleteDataConfirm.deleteButton"]:disabled').should('have.length', 0)

		cy.get('[data-cy="manageData.toggle"]').click()
		cy.get('[data-cy="manageData.table"] [data-cy="deleteDataConfirm.deleteButton"]:disabled').should('have.length', 8)
	})

	it('properly shows run history data', () => {
		cy.route('GET', '/api/flows/newStepInfo', 'fixture:newStepInfo.json')
		cy.route('GET', '/api/entities', 'fixture:entities.json')
		cy.route('GET', '/api/flows/21232f297a57a5a743894a0e4a801fc3', 'fixture:flow-envision.json')
		cy.visit('/integrate')
		cy.get('[data-cy="manageData.table"] tr').should('have.length', 8)
		cy.get('[data-cy="manageData.table"] [data-cy="deleteDataConfirm.deleteButton"]').should('have.length', 8)
		cy.get('[data-cy="manageData.table"] [data-cy="deleteDataConfirm.deleteButton"]:disabled').should('have.length', 6)

		cy.get('[data-cy="jobStatus"]').should('have.length', 0)

		cy.get('[data-cy="manageData.toggleRunHistory"]').click()
		cy.get('[data-cy="jobStatus"]').should('have.length', 2)
		cy.get('[data-cy="manageData.table"] [data-cy="deleteDataConfirm.deleteButton"]:disabled').should('have.length', 0)

		cy.get('[data-cy="manageData.toggle"]').click()
		cy.get('[data-cy="jobStatus"]').should('have.length', 0)
		cy.get('[data-cy="manageData.table"] [data-cy="deleteDataConfirm.deleteButton"]:disabled').should('have.length', 6)

		cy.get('[data-cy="manageData.toggle"]').click()
		cy.get('[data-cy="jobStatus"]').should('have.length', 0)
		cy.get('[data-cy="manageData.table"] [data-cy="deleteDataConfirm.deleteButton"]:disabled').should('have.length', 0)
	})

	it('properly shows flows', () => {
		cy.route('GET', '/api/flows/newStepInfo', 'fixture:newStepInfo.json')
		cy.route('GET', '/api/entities', 'fixture:entities.json')
		cy.route('GET', '/api/flows/21232f297a57a5a743894a0e4a801fc3', 'fixture:flow-envision.json')
		cy.visit('/integrate')

		cy.get('[data-cy="integrate.flowRow"]').should('have.length', 1)
		cy.get('[data-cy="manageData.toggleFlows"]').click()
		cy.get('[data-cy="integrate.flowRow"]').should('have.length', 0)
		cy.get('[data-cy="manageData.toggleFlows"]').click()
		cy.get('[data-cy="integrate.flowRow"]').should('have.length', 1)
	})

	it('add a custom step', () => {
    cy.route('GET', '/api/flows/newStepInfo', 'fixture:newStepInfo.json')
		cy.route('GET', '/api/entities', 'fixture:entities.json')
		cy.route('GET', '/api/flows/21232f297a57a5a743894a0e4a801fc3', 'fixture:flow-envision.json')
		cy.visit('/integrate')
		cy.get('div#MyCustomStep').should('not.exist')
		cy.contains('Customer => MyCustomStep').should('not.exist')
		cy.get('[data-cy="integrate.addStepBtn"]').click();
		cy.get('[data-cy="addStepDialog.stepNameField"]').clear().type('MyCustomStep')
		cy.get('[data-cy="addStepDialog.stepTypeField"]').parentsUntil('.v-select__slot').click()
		cy.get('.stepTypeArray .v-list-item').contains('Custom').parentsUntil('.v-list-item').click()
		cy.get('[data-cy="addStepDialog.entityTypeField"]').parentsUntil('.v-select__slot').click()
		cy.get('.entityTypeArray .v-list-item').contains('Customer').parentsUntil('.v-list-item').click()
		cy.get('[data-cy="addStepDialog.dataSourceField"]').parentsUntil('.v-select__slot').click()
		cy.get('.dataSourceArray .v-list-item').contains('DataSource1').parentsUntil('.v-list-item').click()
		cy.route('GET', '/api/flows/21232f297a57a5a743894a0e4a801fc3', 'fixture:flowWithCustomStep.json')
		cy.route('GET', '/api/flows/customSteps/MyCustomStep', 'fixture:customStep.json')
		cy.get('[data-cy="addStepDialog.saveBtn"]').click();

		cy.wait('@updateStep')
			.its('request.body')
			.should(body => {
				console.log(JSON.stringify(body))
				expect(body).to.deep.equal({"flowName":"21232f297a57a5a743894a0e4a801fc3","step":{"name":"MyCustomStep","description":"","additionalCollections":[],"targetEntityType":"http://marklogic.com/envision/Customer-0.0.1/Customer","sourceDatabase":"data-hub-STAGING","targetDatabase":"data-hub-FINAL","collections":["Customer"],"selectedSource":"query","sourceCollection":"DataSource1","sourceQuery":"cts.collectionQuery([\"DataSource1\"])","permissions":"data-hub-common,read,data-hub-common,update","outputFormat":"json","customHook":{"module":"/envision/customHooks/uriRemapper.sjs","parameters":{},"user":"","runBefore":false},"retryLimit":0,"batchSize":100,"threadCount":4,"stepDefinitionName":"MyCustomStep","stepDefinitionType":"CUSTOM"}})
			})
		cy.get('#MyCustomStep').click();
		cy.contains('Customer => MyCustomStep').should('exist')
		cy.get('div#MyCustomStep').should('exist')

	})
})
