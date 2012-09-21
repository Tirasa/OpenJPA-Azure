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

import java.util.ArrayList;
import java.util.List;
import net.tirasa.jpasqlazure.beans.Gender;
import net.tirasa.jpasqlazure.beans.Person;
import net.tirasa.jpasqlazure.repository.PersonRepository;
import net.tirasa.jpasqlazure.web.pages.panel.ActionLinkPanel;
import net.tirasa.jpasqlazure.web.pages.util.PersonProvider;
import org.apache.wicket.Page;
import org.apache.wicket.Session;
import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.ajax.IAjaxCallDecorator;
import org.apache.wicket.ajax.IAjaxIndicatorAware;
import org.apache.wicket.ajax.calldecorator.AjaxPreprocessingCallDecorator;
import org.apache.wicket.ajax.markup.html.AjaxLink;
import org.apache.wicket.authroles.authorization.strategies.role.Roles;
import org.apache.wicket.authroles.authorization.strategies.role.annotations.AuthorizeInstantiation;
import org.apache.wicket.extensions.ajax.markup.html.IndicatingAjaxLink;
import org.apache.wicket.extensions.ajax.markup.html.modal.ModalWindow;
import org.apache.wicket.extensions.ajax.markup.html.repeater.data.table.AjaxFallbackDefaultDataTable;
import org.apache.wicket.extensions.markup.html.repeater.data.grid.ICellPopulator;
import org.apache.wicket.extensions.markup.html.repeater.data.table.AbstractColumn;
import org.apache.wicket.extensions.markup.html.repeater.data.table.IColumn;
import org.apache.wicket.extensions.markup.html.repeater.data.table.PropertyColumn;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.markup.repeater.Item;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;
import org.apache.wicket.spring.injection.annot.SpringBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AuthorizeInstantiation(Roles.USER)
public class HomePage extends WebPage implements IAjaxIndicatorAware {

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(HomePage.class);

    private static final int MODAL_WIN_HEIGHT = 400;

    private static final int MODAL_WIN_WIDTH = 600;

    private FeedbackPanel feedbackPanel;

    @SpringBean
    private PersonRepository repository;

    private final WebMarkupContainer container;

    private final ModalWindow editModalWin;

    /**
     * Response flag set by the Modal Window after the operation is completed.
     */
    private boolean modalResult = false;

    public HomePage() {
        super();

        // TMP
//        if (!repository.findAll().iterator().hasNext()) {
//            for (int i = 0; i < 12; i++) {
//                Person user = new Person();
//                user.setUsername("Bob" + i);
//                user.setPassword("password");
//                user.setGender(Gender.M);
//                user.setPicture("picture".getBytes());
//                user.setInfo("some info");
//                repository.save(user);
//            }
//        }
        // TMP

        feedbackPanel = new FeedbackPanel("feedback");
        feedbackPanel.setOutputMarkupId(true);
        add(feedbackPanel);

        editModalWin = new ModalWindow("editModalWin");
        editModalWin.setCssClassName(ModalWindow.CSS_CLASS_GRAY);
        editModalWin.setInitialHeight(MODAL_WIN_HEIGHT);
        editModalWin.setInitialWidth(MODAL_WIN_WIDTH);
        editModalWin.setCookieName("edit-modal");
        add(editModalWin);

        List<IColumn> columns = new ArrayList<IColumn>();
        columns.add(new PropertyColumn(new Model("id"), "id", "id"));
        columns.add(new PropertyColumn(new Model("username"), "username", "username"));
        columns.add(new AbstractColumn<Person>(new Model("edit")) {

            @Override
            public void populateItem(final Item<ICellPopulator<Person>> item, final String componentId,
                    final IModel<Person> imodel) {

                ActionLinkPanel panel = new ActionLinkPanel(componentId, imodel, new IndicatingAjaxLink("link") {

                    @Override
                    public void onClick(final AjaxRequestTarget target) {
                        Session.get().cleanupFeedbackMessages();
                        target.add(feedbackPanel);

                        editModalWin.setPageCreator(new ModalWindow.PageCreator() {

                            @Override
                            public Page createPage() {
                                return new EditModalPage(
                                        HomePage.this.getPageReference(), editModalWin, imodel.getObject(), repository);
                            }
                        });
                        editModalWin.setTitle("Person " + imodel.getObject().getId() + " - Edit");
                        editModalWin.show(target);
                    }
                }, "edit");
                item.add(panel);
            }
        });
        columns.add(new AbstractColumn<Person>(new Model("delete")) {

            @Override
            public void populateItem(Item<ICellPopulator<Person>> item, String componentId,
                    final IModel<Person> imodel) {

                ActionLinkPanel panel = new ActionLinkPanel(componentId, imodel, new IndicatingAjaxLink("link") {

                    @Override
                    public void onClick(final AjaxRequestTarget target) {
                        Session.get().cleanupFeedbackMessages();
                        target.add(feedbackPanel);

                        try {
                            repository.delete(imodel.getObject());
                        } catch (Exception e) {
                            LOG.error("While deleting a person", e);
                            error(e.getMessage());
                            return;
                        }

                        info("Operation Succeded");
                        target.add(feedbackPanel);
                        target.add(container);
                    }

                    @Override
                    protected IAjaxCallDecorator getAjaxCallDecorator() {
                        return new AjaxPreprocessingCallDecorator(super.getAjaxCallDecorator()) {

                            private static final long serialVersionUID = -7927968187160354605L;

                            @Override
                            public CharSequence preDecorateScript(final CharSequence script) {
                                return "if (confirm('Are you sure?'))" + "{" + script + "}";
                            }
                        };
                    }
                }, "delete");
                item.add(panel);
            }
        });

        final AjaxFallbackDefaultDataTable table = new AjaxFallbackDefaultDataTable("table", columns,
                new PersonProvider(repository), 10);

        container = new WebMarkupContainer("container");
        container.add(table);
        container.setOutputMarkupId(true);

        add(container);
        setWindowClosedCallback(editModalWin, container);

        AjaxLink createLink = new IndicatingAjaxLink("createLink") {

            @Override
            public void onClick(AjaxRequestTarget target) {
                Session.get().cleanupFeedbackMessages();
                target.add(feedbackPanel);

                editModalWin.setPageCreator(new ModalWindow.PageCreator() {

                    @Override
                    public Page createPage() {
                        return new EditModalPage(HomePage.this.getPageReference(), editModalWin,
                                new Person(), repository);
                    }
                });
                editModalWin.setTitle("New Person");
                editModalWin.show(target);
            }
        };
        add(createLink);
    }

    public boolean isModalResult() {
        return modalResult;
    }

    public void setModalResult(final boolean operationResult) {
        this.modalResult = operationResult;
    }

    /**
     * Set a WindowClosedCallback for a ModalWindow instance.
     *
     * @param window window
     * @param container container
     */
    private void setWindowClosedCallback(final ModalWindow window, final WebMarkupContainer container) {

        window.setWindowClosedCallback(new ModalWindow.WindowClosedCallback() {

            @Override
            public void onClose(final AjaxRequestTarget target) {
                target.add(container);
                if (isModalResult()) {
                    info("Operation Succeded");
                    target.add(feedbackPanel);
                    setModalResult(false);
                }
            }
        });
    }

    @Override
    public String getAjaxIndicatorMarkupId() {
        return "veil";
    }
}
