package auth

type RoleManager interface {
	HasRole(username, role string) bool
}

type UserManager interface {
	GetUser(username string) (User, error)
}

type User struct {
	Username string
	Roles    []string
}
