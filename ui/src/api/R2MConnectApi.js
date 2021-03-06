import axios from 'axios';

export default {
	name: 'R2MConnectAPI',
	dbconnect({collection, database}, files, progressHandler = null) {
		let formData = new FormData()
		formData.append('collection', collection)
		formData.append('database', database)
		for (let i = 0; i < files.length; i++) {
			const file = files[i]
			formData.append('files', file)
		}
		return axios.post( '/api/r2mconnect',
			formData,
			{
				headers: {
					'Content-Type': 'multipart/form-data'
				},
				onUploadProgress: progressHandler
			}
		)
	}
}
