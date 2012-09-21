/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package net.tirasa.jpasqlazure.web.pages;

import java.util.Arrays;
import net.tirasa.jpasqlazure.beans.Gender;
import net.tirasa.jpasqlazure.beans.Person;
import net.tirasa.jpasqlazure.repository.PersonRepository;
import org.apache.wicket.PageReference;
import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.authroles.authorization.strategies.role.annotations.AuthorizeInstantiation;
import org.apache.wicket.extensions.ajax.markup.html.IndicatingAjaxButton;
import org.apache.wicket.extensions.ajax.markup.html.modal.ModalWindow;
import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.form.DropDownChoice;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.form.PasswordTextField;
import org.apache.wicket.markup.html.form.TextArea;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.CompoundPropertyModel;

@AuthorizeInstantiation("USER")
public class EditModalPage extends WebPage {

    private FeedbackPanel feedbackPanel;

    public EditModalPage(final PageReference callPageRef, final ModalWindow window, final Person person,
            final PersonRepository repository) {

        feedbackPanel = new FeedbackPanel("feedback");
        feedbackPanel.setOutputMarkupId(true);
        add(feedbackPanel);

        Form<Person> form = new Form<Person>("form", new CompoundPropertyModel<Person>(person));
        add(form);

        TextField username = new TextField("username");
        username.setRequired(true);
        form.add(username);

        PasswordTextField password = new PasswordTextField("password");
        password.setRequired(false);
        form.add(password);

        DropDownChoice<Gender> gender = new DropDownChoice<Gender>("gender", Arrays.asList(Gender.values()));
        gender.setRequired(true);
        form.add(gender);

        TextArea info = new TextArea("info");
        info.setRequired(false);
        form.add(info);

        IndicatingAjaxButton submit = new IndicatingAjaxButton("save") {

            @Override
            protected void onSubmit(AjaxRequestTarget target, Form<?> form) {
                try {
                    repository.save(person);
                    ((HomePage) callPageRef.getPage()).setModalResult(true);
                    window.close(target);
                } catch (IllegalArgumentException e) {
                    error("Could not save");
                    target.add(feedbackPanel);
                }
            }

            @Override
            protected void onError(AjaxRequestTarget target, Form<?> form) {
                target.add(feedbackPanel);
            }
        };
        form.add(submit);
    }
}
