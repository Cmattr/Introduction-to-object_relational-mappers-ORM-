from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
from marshmallow import fields, ValidationError
from sqlalchemy.sql import func
#Task 1: Setting Up Flask with Flask-SQLAlchemy
# Create connection
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+mysqlconnector://root:Richardson!629@localhost/gym_db'
db = SQLAlchemy(app)
ma = Marshmallow(app)

# Define models
class Members(db.Model):
    __tablename__ = 'members'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    age = db.Column(db.Integer)

class Trainers(db.Model):
    __tablename__ = 'trainers'
    trainer_id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    activity = db.Column(db.String(320))

class WorkOutSessions(db.Model):
    __tablename__ = 'workoutsessions'
    id = db.Column(db.Integer, primary_key=True)
    member_id = db.Column(db.Integer, nullable=False)
    session_date = db.Column(db.String(255), nullable=False)
    session_time = db.Column(db.String(320), nullable=False)
    activity = db.Column(db.String(15))
    trainer_id = db.Column(db.Integer)

# Define schemas
class MembersSchema(ma.Schema):
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    age = fields.Integer(required=True)
    class Meta:
        fields = ("id", "name", "age")

member_schema = MembersSchema()
members_schema = MembersSchema(many=True)

class TrainersSchema(ma.Schema):
    trainer_id = fields.String(required=True)
    name = fields.String(required=True)
    activity = fields.String(required=True)
    class Meta:
        fields = ("trainer_id", "name", "activity")

trainer_schema = TrainersSchema()
trainers_schema = TrainersSchema(many=True)

class WorkOutSessionsSchema(ma.Schema):
    id = fields.Integer(required=True)
    member_id = fields.Integer(required=True)
    session_date = fields.String(required=True)
    session_time = fields.String(required=True)
    activity = fields.String(required=True)
    trainer_id = fields.Integer(required=True)
    class Meta:
        fields = ("id", "member_id", "session_date", "session_time", "activity", "trainer_id")

session_schema = WorkOutSessionsSchema()
sessions_schema = WorkOutSessionsSchema(many=True)
# Task 2: Implementing CRUD Operations for Members Using ORM
# Define routes
@app.route('/members', methods=['GET'])
def get_members():
    members = Members.query.all()
    return members_schema.jsonify(members)

@app.route('/members', methods=['POST'])
def add_member():
    try:
        member_data = member_schema.load(request.json)
    except ValidationError as err:
        return jsonify(err.messages), 400
    new_member = Members(id=member_data["id"], name=member_data['name'], age=member_data['age'])
    db.session.add(new_member)
    db.session.commit()
    return jsonify({"message": "New member added successfully"}), 201

@app.route('/members/<int:id>', methods=['PUT'])
def update_member(id):
    member = Members.query.get_or_404(id)
    try:
        member_data = member_schema.load(request.json)
    except ValidationError as err:
        return jsonify(err.messages), 400
    member.name = member_data['name']
    member.age = member_data['age']
    db.session.commit()
    return jsonify({"message": "Member detail updated successfully"}), 200

@app.route('/members/<int:id>', methods=['DELETE'])
def delete_member(id):
    member = Members.query.get_or_404(id)
    db.session.delete(member)
    db.session.commit()
    return jsonify({"message": "Member removed successfully"}), 200

@app.route('/trainers', methods=['GET'])
def get_trainers():
    trainers = Trainers.query.all()
    return trainers_schema.jsonify(trainers)

@app.route('/trainers', methods=['POST'])
def add_trainer():
    try:
        trainer_data = trainer_schema.load(request.json)
    except ValidationError as err:
        return jsonify(err.messages), 400
    new_trainer = Trainers(trainer_id=trainer_data["trainer_id"], name=trainer_data['name'], activity=trainer_data['activity'])
    db.session.add(new_trainer)
    db.session.commit()
    return jsonify({"message": "New trainer added successfully"}), 201

@app.route('/trainers/<int:id>', methods=['PUT'])
def update_trainer(id):
    trainer = Trainers.query.get_or_404(id)
    try:
        trainer_data = trainer_schema.load(request.json)
    except ValidationError as err:
        return jsonify(err.messages), 400
    trainer.name = trainer_data['name']
    trainer.activity = trainer_data['activity']
    db.session.commit()
    return jsonify({"message": "Trainer detail updated successfully"}), 200

@app.route('/trainers/<int:id>', methods=['DELETE'])
def delete_trainer(id):
    trainer = Trainers.query.get_or_404(id)
    db.session.delete(trainer)
    db.session.commit()
    return jsonify({"message": "Trainer removed successfully"}), 200

#Task 3: Managing Workout Sessions with ORM
@app.route('/sessions', methods=['GET'])
def get_sessions():
    sessions = WorkOutSessions.query.all()
    return sessions_schema.jsonify(sessions)

@app.route('/sessions', methods=['POST'])
def add_session():
    try:
        session_data = session_schema.load(request.json)
    except ValidationError as err:
        return jsonify(err.messages), 400
    new_session = WorkOutSessions(id=session_data["id"], member_id=session_data['member_id'], session_date=session_data['session_date'], session_time=session_data['session_time'], activity=session_data['activity'], trainer_id=session_data['trainer_id'])
    db.session.add(new_session)
    db.session.commit()
    return jsonify({"message": "New Session added successfully"}), 201

@app.route('/sessions/<int:id>', methods=['PUT'])
def update_session(id):
    session = WorkOutSessions.query.get_or_404(id)
    try:
        session_data = session_schema.load(request.json)
    except ValidationError as err:
        return jsonify(err.messages), 400
    session.member_id = session_data['member_id']
    session.session_date = session_data['session_date']
    session.session_time = session_data['session_time']
    session.activity = session_data['activity']
    session.trainer_id = session_data['trainer_id']
    db.session.commit()
    return jsonify({"message": "Session detail updated successfully"}), 200

@app.route('/sessions/<int:id>', methods=['DELETE'])
def delete_session(id):
    session = WorkOutSessions.query.get_or_404(id)
    db.session.delete(session)
    db.session.commit()
    return jsonify({"message": "Session removed successfully"}), 200

#Task 1: SQL DISTINCT Usage
@app.route('/members/<int:member_id>/sessions', methods=['GET'])
def get_sessions_by_member(member_id):
    sessions = WorkOutSessions.query.filter_by(member_id=member_id).all()
    return sessions_schema.jsonify(sessions)

#Task 2: SQL COUNT Functionality
@app.route('/distinct-trainers', methods=['GET'])
def get_distinct_trainers():
    distinct_trainers = db.session.query(Trainers).join(WorkOutSessions, Trainers.trainer_id == WorkOutSessions.trainer_id).distinct().all()
    return trainers_schema.jsonify(distinct_trainers)

@app.route('/trainers/members-count', methods=['GET'])
def get_trainer_member_counts():
    results = db.session.query(
        Trainers.name,
        func.count(func.distinct(WorkOutSessions.member_id)).label('member_count')
    ).join(WorkOutSessions, Trainers.trainer_id == WorkOutSessions.trainer_id).group_by(Trainers.trainer_id).all()
    
    trainer_member_counts = [{"trainer": result.name, "member_count": result.member_count} for result in results]
    return jsonify(trainer_member_counts)

#Task 3: SQL BETWEEN Usage
@app.route('/members/age-range', methods=['GET'])
def get_members_age_range():
    min_age = request.args.get('min_age', default=25, type=int)
    max_age = request.args.get('max_age', default=30, type=int)
    members_in_age_range = Members.query.filter(Members.age >= min_age, Members.age <= max_age).all()
    return members_schema.jsonify(members_in_age_range)

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True)