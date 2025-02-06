package auth

type RoleManager interface {
	HasRole(username, role string) bool
}
