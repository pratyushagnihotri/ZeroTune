db = db.getSiblingDB('admin');
db.auth('dsps', 'qvajv')
db = db.getSiblingDB('plangeneratorflink')

db.createUser(
        {
            user: "pgf",
            pwd: "pwd",
            roles: [
                {
                    role: "readWrite",
                    db: "plangeneratorflink"
                }
            ]
        }
);
