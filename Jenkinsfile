node ('tox' && 'root') {
	stage name: 'Checkout'
	checkout scm
	stage name: 'Test'
	sh 'sudo tox -e destroy_machine_with_sudo -- -v'
}
